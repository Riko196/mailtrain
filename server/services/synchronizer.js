'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const config = require('../lib/config');
const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const { CampaignStatus } = require('../../shared/campaigns');
const { MessageStatus } = require('../../shared/messages');
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

    /** 
     * Returns true if there is no available task.
     */
    noScheduledTask() {
        return !this.synchronizingPausingCampaigns.length &&
             !this.synchronizingScheduledCampaigns.length &&
             !this.synchronizingQueuedMessages.size;
    }

    /**
     * The main infinite loop where synchronizer always tries to synchronize things from Mailtrain to MongoDB
     * and then tries synchronize results from MongoDB backward to Mailtrain.
     */
    async synchronizerLoop() {
        log.verbose('Synchronizer', 'Starting loop...');

        while (!this.stopWorking) {
            /* Variable that tells us whether there is some task from MongoDB for synchronizing */
            this.noSynchronizingTask = true;
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
                /* MongoDB --> Mailtrain */
                await this.synchronizeMessyMessagesFromMongoDB();

                if (this.noScheduledTask() && this.noSynchronizingTask) {
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

    /**
     * Called by client when he does some campaign operations and we don't want to wait for the next periodic check.
     */
    async callImmediateScheduleCheck() {
        this.scheduler.scheduleCheck();
    }

    /**
     * Select first campaign from list of pausing campaigns.
     */
    selectPausingCampaign() {
        return this.synchronizingPausingCampaigns.shift();
    }

    /**
     * Select first campaign from list of pausing campaigns and update campaign status to PAUSED.
     */
    async synchronizePausingCampaigns() {
        const campaignId = this.selectPausingCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with pausing campaignId: ${campaignId}`);
            await campaigns.paused(contextHelpers.getAdminContext(), campaignId);
            /* We rather delete a task from MongoDB although we have only paused it because it will be scheduled again if a client presses continue  */
            await this.mongodb.collection('tasks').deleteMany({ 'campaign.id': campaignId });
            log.verbose('Synchronizer', `Pausing campaignId: ${campaignId} successfully synchronized with MongoDB!`);
        }
    }

    /**
     * Select first campaign from list of scheduled campaigns.
     */
    selectScheduledCampaign() {
        return this.synchronizingScheduledCampaigns.shift();
    }

    /**
     * Select first campaign from list of scheduled campaigns, collect all needed data, send them to MongoDB, and update campaign status to SENDING.
     */
    async synchronizeScheduledCampaigns() {
        const campaignId = this.selectScheduledCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with scheduled campaignId: ${campaignId}`);
            /* Collect all needed campaign data for sending */
            const campaignData = await this.dataCollector.collectData({
                type: MessageType.REGULAR,
                campaignId
            });

            /* Set campaign status to SENDING and insert task into the MongoDB database */
            await campaigns.send(contextHelpers.getAdminContext(), campaignId);
            await this.mongodb.collection('tasks').insertOne(campaignData);
            log.verbose('Synchronizer', `New task with campaignId: ${campaignId} successfully sent to MongoDB!`);
        }
    }

    /**
     * Select first CHUNK_SIZE of queued messages grouped by send configuration.
     */
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

    /**
     * Select first CHUNK_SIZE of queued messages grouped by send configuration, collect all needed data, send them to MongoDB, and delete them from MySQL.
     */
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
            collectedMessageData._id = queuedMessage.id;
            preparedQueuedMessages.push(collectedMessageData);
        }

        await this.mongodb.collection('queued').insertMany(preparedQueuedMessages);
        log.verbose('Synchronizer', 'Queued messages successfully sent to MongoDB!');

        /* Set as SENT although it has not been yet (for rescheduling purpose) */
        const updatingIds = scheduledQueuedMessages.map(queuedMessage => queuedMessage.id);
        await knex('queued').whereIn('id', updatingIds).update({
            status: MessageStatus.SENT
        });
    }

    /**
     * Synchronize all data from sending campaigns from MongoDB (it includes campaign links, events about clicked links, count of sent messages, sent messages).
     */
    async synchronizeSendingCampaignsFromMongoDB() {
        await this.synchronizeLinksFromMongoDB();
        await this.synchronizeClickedLinksFromMongoDB();
        await this.synchronizeCountOfSentCampaignMessages();
        await this.synchronizeCampaignMessagesFromMongoDB();

        /* Find FINISHED campaigns and remove them from tasks */
        const sendingCampaigns = await this.mongodb.collection('tasks').find({}).limit(CHUNK_SIZE).toArray();
        for (const sendingCampaign of sendingCampaigns) {
            const campaignId = sendingCampaign.campaign.id;

            /* Check whether no error occurred during sending campaign and if so then postpone sending this campaign */
            if (sendingCampaign.withErrors) {
                log.error('Synchronizer', `Error occurred during sending campaign with id: ${campaignId}, it is postponing!`);
                this.scheduler.postponeSendConfigurationId(sendingCampaign.sendConfiguration.id, sendingCampaign.withErrorsUpdated);
                /* Set problematic campaign as SCHEDULED and Scheduler will schedule it after currently set postponed period  */
                await campaigns.reschedule(contextHelpers.getAdminContext(), campaignId);
                await this.mongodb.collection('tasks').deleteMany({ _id: sendingCampaign._id });
                continue;
            }

            /* If all campaign messages have been sent, then remove task and set campaign status FINISHED */
            const remainingCampaignMessages = await this.mongodb.collection('campaign_messages').countDocuments({
                campaign: campaignId
            });

            if (!remainingCampaignMessages) {
                log.verbose('Synchronizer', `Campaign with id: ${campaignId} is finished!`);
                /* We rather call also this update campaign status although campaign is already set as FINISHED (there could appear some problem with delivered messages) */
                await campaigns.finish(contextHelpers.getAdminContext(), campaignId);
                await this.mongodb.collection('tasks').deleteMany({ _id: sendingCampaign._id });
            }
        }
    }

    /** 
     * Synchronize all initialized links.
     */
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

    /**
     * Synchronize all data when subscribers opened mail or clicked on some links.
     */
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

    /* 
     * Synchronize count of currently sent campaign messages from MongoDB and MySQL.
     */
    async synchronizeCountOfSentCampaignMessages() {
        const sendingCampaigns = await knex('campaigns').where({ status: CampaignStatus.SENDING });

        for (const sendingCampaign of sendingCampaigns) {
            const campaignId = sendingCampaign.id;

            /* Find count of successfully sent and blacklisted messages from MySQL and MongoDB */
            const counts = {
                mysqlSuccessfullySentCount: await campaigns.getSuccessfullySentCampaignMessagesCount(campaignId),
                mongodbSuccessfullySentCount: await campaigns.getSuccessfullySentCampaignMessagesCountMongoDB(campaignId),
                mysqlBlacklistedCount: await campaigns.getBlacklistedCampaignMessagesCount(campaignId),
                mongodbBlacklistedSentCount: await campaigns.getBlacklistedCampaignMessagesCountMongoDB(campaignId)
            };

            const delivered = counts.mysqlSuccessfullySentCount.count + counts.mongodbSuccessfullySentCount;
            /* Update value of delivered messages */
            await knex('campaigns').where('id', campaignId).update({ delivered });
            const blacklisted = counts.mysqlBlacklistedCount.count + counts.mongodbBlacklistedSentCount;
            /* Update value of blacklisted messages */
            await knex('campaigns').where('id', campaignId).update({ blacklisted });

            /* Find count of all existing messages from MySQL */
            const allMessages = await knex('campaign_messages').where({ campaign: campaignId }).count('* as count').first();

            /* If all messages are sent then update campaign status */
            if (blacklisted + delivered === allMessages.count) {
                await campaigns.finish(contextHelpers.getAdminContext(), campaignId);
            }
        }
    }

    /** 
     * Synchronize all campaign messages from MongoDB and do the final processing (update campaign_messages table).
     */
    async synchronizeCampaignMessagesFromMongoDB() {
        await this.synchronizeSentCampaignMessagesFromMongoDB();
        await this.synchronizeFailedCampaignMessagesFromMongoDB();
    }

    /**
     *  Synchronize all sent campaign messages from MongoDB and do the final processing (update campaign_messages table).
     */
    async synchronizeSentCampaignMessagesFromMongoDB() {
        const sentCampaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: MessageStatus.SENT,
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

        if (sentCampaignMessages.length === 0) {
            return;
        }

        this.noSynchronizingTask = false;

        log.verbose('Synchronizer', `Received ${sentCampaignMessages.length} sent messages from MongoDB!`);
        for (const sentCampaignMessage of sentCampaignMessages) {
            const { _id, campaign, send_configuration, status, response, responseId, updated } = sentCampaignMessage;
            this.scheduler.resetSendConfigurationRetryCount(send_configuration, updated);
            await campaigns.updateMessageResponse(contextHelpers.getAdminContext(), _id, campaign, status, response, responseId);
        }

        const deletingIds = sentCampaignMessages.map(sentCampaignMessage => sentCampaignMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ _id: { $in: deletingIds } });
    }

    /** 
     * Synchronize all failed campaign messages from MongoDB and do the final processing (update campaign_messages table).
     */
    async synchronizeFailedCampaignMessagesFromMongoDB() {
        const failedCampaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: { $in: [MessageStatus.BOUNCED, MessageStatus.COMPLAINED, MessageStatus.FAILED] }
        }).limit(CHUNK_SIZE).toArray();

        if (failedCampaignMessages.length === 0) {
            return;
        }

        this.noSynchronizingTask = false;

        log.verbose('Synchronizer', `Received ${failedCampaignMessages.length} failed messages from MongoDB!`);
        for (const failedCampaignMessage of failedCampaignMessages) {
            const { _id, campaign, status, response, responseId } = failedCampaignMessage;
            await campaigns.updateMessageResponse(contextHelpers.getAdminContext(), _id, campaign, status, response, responseId);
        }

        const deletingIds = failedCampaignMessages.map(failedCampaignMessage => failedCampaignMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ _id: { $in: deletingIds } });
    }

    /**
     * Synchronize all sent queued messages from MongoDB and do the final processing.
     */
    async synchronizeSentQueuedMessagesFromMongoDB() {
        //log.verbose('Synchronizer', 'Synchronizing sent queued messages from MongoDB...');
        const queuedMessages = await this.mongodb.collection('queued').find({
            status: { $in: [MessageStatus.SENT, MessageStatus.FAILED] },
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

        if (queuedMessages.length === 0) {
            return;
        }

        this.noSynchronizingTask = false;

        for (const queuedMessage of queuedMessages) {
            if (queuedMessage.attachments) {
                await this.unlockAttachments(queuedMessage.attachments);
            }

            if (queuedMessage.withErrors) {
                this.scheduler.postponeSendConfigurationId(queuedMessage.sendConfiguration, queuedMessage.updated);
                await this.mongodb.collection('queued').deleteOne({ _id: queuedMessage._id });
                await knex('queued').where('id', queuedMessage._id).update('status', MessageStatus.SCHEDULED);
                continue;
            } else {
                await knex('queued').where('id', queuedMessage._id).del();
            }

            if (queuedMessage.type === MessageType.TRIGGERED) {
                await this.processSentTriggeredMessage(queuedMessage);
            }

            if (queuedMessage.campaign && queuedMessage.type === MessageType.TEST) {
                await this.processSentCampaignTestMessage(queuedMessage);
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
                status: MessageStatus.SENT,
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

   /**
    * Insert an entry to test_messages. This allows us to remember test sends to lists that are not
    * listed in the campaign - see the check in getMessage.
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

    /**
     * Unlock all attachments (at semaphore) used in sent queued message.
     */
    async unlockAttachments(attachments) {
        for (const attachment of attachments) {
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
        }
    }

    /**
     * Synchronize all messy messages (set status but no response for too long time).
     */
     async synchronizeMessyMessagesFromMongoDB() {
        /* Campaign messages */
        const campaignMessyMessages = await this.getMessyMessagesFromMongoDB('campaign_messages');
        /* Update status to FAILED and remove messy messages */
        for (const campaignMessyMessage of campaignMessyMessages) {
            const { _id, campaign, response, responseId } = campaignMessyMessage;
            await campaigns.updateMessageResponse(contextHelpers.getAdminContext(), _id, campaign, MessageStatus.FAILED, response, responseId);
        }
        let deletingIds = campaignMessyMessages.map(campaignMessyMessage => campaignMessyMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ _id: { $in: deletingIds } });
        
        /* QUEUED messages */
        const queuedMessyMessages = await this.getMessyMessagesFromMongoDB('queued');
        /* In QUEUED messages we do not need to update status and response, just unlock attachments and remove messy messages */
        for (const queuedMessyMessage of queuedMessyMessages) {
            if (queuedMessyMessage.attachments) {
                await this.unlockAttachments(queuedMessyMessage.attachments);
            }
        }
        deletingIds = queuedMessyMessages.map(queuedMessyMessage => queuedMessyMessage._id);
        await this.mongodb.collection('queued').deleteMany({ _id: { $in: deletingIds } });
    }

    /**
     * Get all messy messages from collection collectionName.
     */
    async getMessyMessagesFromMongoDB(collectionName) {
        /* Constant which specifies how long has to be old messy message so that it woulde be synchronized */
        const MESSY_DIFFERENCE = 86400;
    
        return await this.mongodb.collection(collectionName).aggregate([ {
                $addFields: { updatedDifference: { $subtract: [new Date(), '$updated'] } }
            }, {  
                $match: {
                    $and: [ 
                        { status: { $ne: MessageStatus.SCHEDULED }, },
                        { response: null },
                        { updatedDifference: { $gte: MESSY_DIFFERENCE } }
                    ] 
                } 
            }
        ]).limit(CHUNK_SIZE).toArray();
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

/**
 * The method which is called as first when the synchronizer process is spawned by Mailtrain.
 */
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

/** noinspection JSIgnoredPromiseFromCall */
spawnSynchronizer();
