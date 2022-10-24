'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const config = require('../lib/config');
const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');
const log = require('../lib/log');
const links = require('../models/links');
const campaigns = require('../models/campaigns');
const DataCollector = require('../lib/sender/synchronizer/data-collector');
const Scheduler = require('../lib/sender/synchronizer/scheduler');
const contextHelpers = require('../lib/context-helpers');

/** Chunk of entities which will be processed in one query. */
const CHUNK_SIZE = 10000;

/**
 * The main component for synchronizing between non-high-available centralized Mailtrain and high-available
 * distributed Sender and vice versa. It initializes Scheduler and DataCollector and then in loop it communicates
 * with MongoDB database. It takes data from DataCollector and then sends them to MongoDB database at once for
 * ensuring high-availability.
 */
class Synchronizer {
    constructor() {
        log.verbose('Synchronizer', 'Init synchronizer...');
        /* Pausing campaigns from scheduler for synchronizing */
        this.synchronizingPausingCampaigns = [];
        /* Scheduled campaigns from scheduler for synchronizing */
        this.synchronizingScheduledCampaigns = [];
        /* sendConfigurationId -> [queuedMessage] */
        this.synchronizingQueuedMessages = new Map();
        this.notifier = new Notifier();
        this.dataCollector = new DataCollector();
        /* Value which says whether worker should stop after accomplishing currently executing iteration */
        this.stopWorking = false;
        /* Get MongoDB connection */
        this.mongodb = getMongoDB();
        this.scheduler = new Scheduler(
            this.synchronizingPausingCampaigns,
            this.synchronizingScheduledCampaigns,
            this.synchronizingQueuedMessages,
            this.notifier
        );
        setImmediate(this.synchronizerLoop.bind(this));
    }

    /* Returns true if there is no available task. */
    noTaskAvailable() {
        return !this.synchronizingPausingCampaigns.length &&
             !this.synchronizingScheduledCampaigns.length &&
             !this.synchronizingQueuedMessages.size;
    }

    /*
     * The main infinite loop where synchronizer always tries to synchronize things from Mailtrain to MongoDB
     * and then tries synchronize results from MongoDB backward to Mailtrain.
     */
    async synchronizerLoop() {
        log.verbose('Synchronizer', 'Starting loop...');

        while (!this.stopWorking) {
            try {
                /* Mailtrain --> MongoDB */
                await this.synchronizePausingCampaigns();
                /* Mailtrain --> MongoDB */
                await this.synchronizeScheduledCampaigns();
                /* Mailtrain --> MongoDB */
                await this.synchronizeScheduledQueuedMessages();
                /* MongoDB --> Mailtrain */
                await this.synchronizeSendingCampaignsFromMongoDB();
                /* MongoDB --> Mailtrain */
                await this.synchronizeSentQueuedMessagesFromMongoDB();

                if (this.noTaskAvailable()) {
                    await this.notifier.waitFor('taskAvailable');
                }
            } catch(error) {
                log.error('Synchronizer', `Synchronizing failed with error: ${error.message}`);
                log.error(error.stack);
            }
        }

        log.info('Synchronizer', `Synchronizer killed after successfully completed work!`);
        process.exit(0);
    }

    /* Called by client when he does some campaign operations and we don't want to wait for the next periodic check. */
    async callImmediateScheduleCheck() {
        this.scheduler.periodicCheck();
    }

    selectPausingCampaign() {
        return this.synchronizingPausingCampaigns.shift();
    }

    async synchronizePausingCampaigns() {
        const campaignId = this.selectPausingCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with pausing campaignId: ${campaignId}`);
            /* We rather delete a task from MongoDB although we have only paused it because it will be scheduled again if a client presses continue  */
            await this.mongodb.collection('tasks').deleteMany({ 'campaign.id': campaignId });
            log.verbose('Synchronizer', `Pausing campaignId: ${campaignId} successfully synchronized with MongoDB!`);
            await campaigns.updateCampaignStatus(contextHelpers.getAdminContext(), campaignId, CampaignStatus.PAUSED);
        }
    }

    selectScheduledCampaign() {
        return this.synchronizingScheduledCampaigns.shift();
    }

    async synchronizeScheduledCampaigns() {
        const campaignId = this.selectScheduledCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with scheduled campaignId: ${campaignId}`);
            /* Collect all needed campaign data for sending */
            const campaignData = await this.dataCollector.collectData({
                type: MessageType.REGULAR,
                campaignId
            });

            await this.mongodb.collection('tasks').insertOne(campaignData);
            log.verbose('Synchronizer', `New task with campaignId: ${campaignId} successfully sent to MongoDB!`);
            await campaigns.updateCampaignStatus(contextHelpers.getAdminContext(), campaignId, CampaignStatus.SENDING);
        }
    }

    selectScheduledQueuedMessages() {
        let scheduledQueuedMessages = [];
        for (const [key, value] of this.synchronizingQueuedMessages) {
            scheduledQueuedMessages = scheduledQueuedMessages.concat(value.splice(0, CHUNK_SIZE));
            if (scheduledQueuedMessages.length > CHUNK_SIZE) {
                break;
            }

            if (!this.synchronizingQueuedMessages.get(key).length) {
                this.synchronizingQueuedMessages.delete(key);
            }
        }
        return scheduledQueuedMessages;
    }

    async synchronizeScheduledQueuedMessages() {
        const scheduledQueuedMessages = this.selectScheduledQueuedMessages();

        if (scheduledQueuedMessages.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Scheduled new queued messages!`);

        const preparedQueuedMessages = [];
        /* Collect all needed data for each queued message prepared for sending */
        for (const queuedMessage of scheduledQueuedMessages) {
            const data = queuedMessage.data;
            /* Merge message data with additional needed data and make query for DataCollector */
            const query = Object.assign(data, 
                { type: queuedMessage.type, listId: data.list, subscriptionId: data.subscription, sendConfigurationId: queuedMessage.send_configuration});
            
            /* Collect all needed data for this queued message and add it to the list */
            const collectedMessageData = await this.dataCollector.collectData(query);
            preparedQueuedMessages.push(collectedMessageData);
        }

        await this.mongodb.collection('queued').insertMany(preparedQueuedMessages);
        log.verbose('Synchronizer', 'Queued messages successfully sent to MongoDB!');

        /* Remove all sent messages */
        const deletingIds = scheduledQueuedMessages.map(queuedMessage => queuedMessage.id);
        await knex('queued').whereIn('id', deletingIds).del();
    }

    async synchronizeSendingCampaignsFromMongoDB() {
        await this.synchronizeLinksFromMongoDB();
        await this.synchronizeClickedLinksFromMongoDB();
        await this.synchronizeCountOfSentCampaignMessages();
        await this.synchronizeCampaignMessagesFromMongoDB();

        /* Find FINISHED campaigns and remove them from tasks */
        const sendingCampaigns = await this.mongodb.collection('tasks').find({}).limit(CHUNK_SIZE).toArray();
        for (const sendingCampaign of sendingCampaigns) {
            const campaignId = sendingCampaign.campaign.id;
            const remainingCampaignMessages = await this.mongodb.collection('campaign_messages').countDocuments({
                campaign: campaignId
            });

            if (!remainingCampaignMessages) {
                log.verbose('Synchronizer', `Campaign with id: ${campaignId} is finished!`);
                this.scheduler.checkSentErrors(sendingCampaign.sendConfiguration, sendingCampaign.withErrors);
                await this.mongodb.collection('tasks').deleteMany({ _id: sendingCampaign._id });
                /* We rather call also this update campaign status although campaign is already set as FINISHED (there could appear some problem with delivered messages) */
                await campaigns.updateCampaignStatus(contextHelpers.getAdminContext(), campaignId, CampaignStatus.FINISHED);
            }
        }
    }

    /* Synchronize all initialized links */
    async synchronizeLinksFromMongoDB() {
        const unsynchronizedLinks = await this.mongodb.collection('links').find({
            status: links.LinkStatus.UNSYNCHRONIZED
        }).limit(CHUNK_SIZE).toArray();

        if (unsynchronizedLinks.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Received ${unsynchronizedLinks.length} links from MongoDB!`);

        for (const link of unsynchronizedLinks) {
            await this.mongodb.collection('links').updateOne({ _id: link._id }, { $set: { status: links.LinkStatus.SYNCHRONIZED } });
            delete link._id, link.status;
            await links.insertLinkIfNotExists(link);
        }
    }

    /* Synchronize all data when subscribers opened mail or clicked on some links */
    async synchronizeClickedLinksFromMongoDB() {
        const clickedLinks = await this.mongodb.collection('campaign_links').find({}).limit(CHUNK_SIZE).toArray();

        if (clickedLinks.length === 0) {
            return;
        }

        for (const clickedLink of clickedLinks) {
            await links.countLink(clickedLink.ip, clickedLink.header, clickedLink.campaign, clickedLink.list,
                clickedLink.subscription, clickedLink.linkId);
        }

        const deletingIds = clickedLinks.map(clickedLink => clickedLink._id);
        await this.mongodb.collection('campaign_links').deleteMany({ _id: { $in: deletingIds } });
    }

    /* Synchronize count of currently sent campaign messages from MongoDB and MySQL */
    async synchronizeCountOfSentCampaignMessages() {
        const sendingCampaigns = await knex('campaigns').where({ status: CampaignStatus.SENDING });

        for (const sendingCampaign of sendingCampaigns) {
            const campaignId = sendingCampaign.id;

            /* Find count of sent messages from MySQL */
            const mysqlSentCount = await knex('campaign_messages').where({ 
                campaign: campaignId,
                status: CampaignMessageStatus.SENT
            }).count('* as count').first();

            /* Find count of sent messages from MongoDB */
            const mongodbSentCount = await this.mongodb.collection('campaign_messages').countDocuments({
                campaign: campaignId,
                status: CampaignMessageStatus.SENT,
                response: { $ne: null }
            });

            const delivered = mysqlSentCount.count + mongodbSentCount;

            /* Find count of all messages from MySQL */
            const allMessages = await knex('campaign_messages').where({ campaign: campaignId }).count('* as count').first();

            /* Update value of delivered messages */
            await knex('campaigns').where('id', campaignId).update({ delivered });

            /* If all messages are sent then update campaign status */
            if (delivered === allMessages.count) {
                await campaigns.updateCampaignStatus(contextHelpers.getAdminContext(), campaignId, CampaignStatus.FINISHED);
            }
        }
    }

    /* Synchronize all campaign messages from MongoDB and do the final processing (update campaign_messages table) */
    async synchronizeCampaignMessagesFromMongoDB() {
        await this.synchronizeSentCampaignMessagesFromMongoDB();
        await this.synchronizeFailedCampaignMessagesFromMongoDB();
    }

    /* Synchronize all sent campaign messages from MongoDB and do the final processing (update campaign_messages table) */
    async synchronizeSentCampaignMessagesFromMongoDB() {
        const sentCampaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: CampaignMessageStatus.SENT,
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

        if (sentCampaignMessages.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Received ${sentCampaignMessages.length} sent messages from MongoDB!`);
        for (const sentCampaignMessage of sentCampaignMessages) {
            const { _id, campaign, status, response, responseId } = sentCampaignMessage;
            await campaigns.updateMessageResponse(contextHelpers.getAdminContext(), _id, campaign, status, response, responseId);
        }

        const deletingIds = sentCampaignMessages.map(sentCampaignMessage => sentCampaignMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ _id: { $in: deletingIds } });
    }

    /* Synchronize all failed campaign messages from MongoDB and do the final processing (update campaign_messages table) */
    async synchronizeFailedCampaignMessagesFromMongoDB() {
        const failedCampaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: CampaignMessageStatus.FAILED
        }).limit(CHUNK_SIZE).toArray();

        if (failedCampaignMessages.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Received ${failedCampaignMessages.length} failed messages from MongoDB!`);
        for (const failedCampaignMessage of failedCampaignMessages) {
            const { _id, campaign, status, response, responseId } = failedCampaignMessage;
            await campaigns.updateMessageResponse(contextHelpers.getAdminContext(), _id, campaign, status, response, responseId);
        }

        const deletingIds = failedCampaignMessages.map(failedCampaignMessage => failedCampaignMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ _id: { $in: deletingIds } });
    }

    /* Synchronize all sent queued messages from MongoDB and do the final processing */
    async synchronizeSentQueuedMessagesFromMongoDB() {
        //log.verbose('Synchronizer', 'Synchronizing sent queued messages from MongoDB...');
        const queuedMessages = await this.mongodb.collection('queued').find({
            status: { $in: [CampaignMessageStatus.SENT, CampaignMessageStatus.FAILED] },
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

        for (const queuedMessage of queuedMessages) {
            this.scheduler.checkSentErrors(queuedMessage.sendConfiguration, queuedMessage.withErrors);

            if (queuedMessage.type === MessageType.TRIGGERED) {
                await this.processSentTriggeredMessage(queuedMessage);
            }

            if (queuedMessage.campaign && queuedMessage.type === MessageType.TEST) {
                await this.processSentCampaignTestMessage(queuedMessage);
            }

            if (queuedMessage.attachments) {
                await this.unlockAttachments(queuedMessage.attachments);
            }
        }

        const deletingIds = queuedMessages.map(queuedMessage => queuedMessage._id);
        await this.mongodb.collection('queued').deleteMany({ _id: { $in: deletingIds } });
    }

    async processSentTriggeredMessage(triggeredMessage) {
        try {
            await knex('campaign_messages').insert({
                campaign: triggeredMessage.campaign.id,
                list: triggeredMessage.listId,
                subscription: triggeredMessage.subscriptionId,
                send_configuration: triggeredMessage.sendConfiguration.id,
                status: CampaignMessageStatus.SENT,
                response: triggeredMessage.response,
                response_id: triggeredMessage.response_id,
                updated: new Date(),
                hash_email: triggeredMessage.hashEmail
            });

            await knex('campaigns').where('id', triggeredMessage.campaign.id).increment('delivered');
        } catch (error) {
            /* The entry is already there, so we can ignore this error */
            if (error.code !== 'ER_DUP_ENTRY') {
                throw error;
            }
        }
    }

   /*
    * Insert an entry to test_messages. This allows us to remember test sends to lists that are not
    * listed in the campaign - see the check in getMessage
    */
    async processSentCampaignTestMessage(testMessage) {
        try {
            await knex('test_messages').insert({
                campaign: testMessage.campaign.id,
                list: testMessage.list,
                subscription: testMessage.subscription
            });
        } catch (error) {
            /* The entry is already there, so we can ignore this error */
            if (error.code !== 'ER_DUP_ENTRY') {
                throw error;
            }
        }
    }

    /* Unlock all attachments (at semaphore) used in sent queued message. */
    async unlockAttachments(attachments) {
        attachments.forEach(async (attachment) => {
            /* This means that it is an attachment recorded in table files_campaign_attachment */
            if (attachment.id) {
                try {
                    /* We ignore any errors here because we already sent the message. Thus we have to
                     * mark it as completed to avoid sending it again. */
                    await knex.transaction(async tx => {
                        await files.unlockTx(tx, 'campaign', 'attachment', attachment.id);
                    });
                } catch (error) {
                    log.error('Synchronizer',
                        `Error when unlocking attachment ${attachment.id} for ${result.email} (queuedId: ${queuedMessage.id})`);
                    log.verbose(error.stack);
                }
            }
        });
    }
}

/**
 * Auxiliary class which sleeps and wakes up Synchronizer and Scheduler when some event occurs.
 */
class Notifier {
    constructor() {
        this.conts = new Map();
    }

    notify(id) {
        const cont = this.conts.get(id);
        if (cont) {
            for (const cb of cont) {
                setImmediate(cb);
            }
            this.conts.delete(id);
        }
    }

    async waitFor(id) {
        let cont = this.conts.get(id);
        if (!cont) {
            cont = [];
        }

        const notified = new Promise(resolve => {
            cont.push(resolve);
        });

        this.conts.set(id, cont);

        await notified;
    }
}

/* The method which is called as first when the synchronizer process is spawned by mailtrain. */
async function spawnSynchronizer() {
    /* Connect to the MongoDB and accomplish setup */
    await connectToMongoDB();
    const synchronizer = new Synchronizer();

    /* Catch Ctrl+C */
    process.on('SIGINT', () => {}); 
    /* Catch kill process */
    process.on('SIGTERM', () => {}); 

    process.on('message', msg => {
        if (msg) {
            const type = msg.type;

            if (type === 'schedule-check') {
                /* noinspection JSIgnoredPromiseFromCall */
                synchronizer.callImmediateScheduleCheck();
            }
        }

        /* Catch kill message from Mailtrain root process */
        if (msg === 'exit') {
            synchronizer.stopWorking = true;
            synchronizer.notifier.notify('taskAvailable');
        }
    });

    if (config.title) {
        process.title = config.title + ': synchronizer';
    }

    process.send({ type: 'synchronizer-started' });
}

/* noinspection JSIgnoredPromiseFromCall */
spawnSynchronizer();
