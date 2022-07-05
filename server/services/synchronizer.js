'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const config = require('../lib/config');
const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');
const activityLog = require('../lib/activity-log');
const { CampaignActivityType } = require('../../shared/activity-log');
const log = require('../lib/log');
const { LinkStatus, insertIfNotExists } = require('../models/links');
const DataCollector = require('../lib/sender/synchronizer/data-collector');
const Scheduler = require('../lib/sender/synchronizer/scheduler');

const CHUNK_SIZE = 1000;

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

        while (true) {
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
                log.verbose(error.stack);
            }
        }
    }

    /* Called by client when he does some campaign operations and we don't want to wait for the next periodic check. */
    async callImmediateScheduleCheck() {
        await this.scheduler.periodicCheck();
    }

    selectPausingCampaign() {
        return this.synchronizingPausingCampaigns.shift();
    }

    async synchronizePausingCampaigns() {
        const campaignId = this.selectPausingCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with pausing campaignId: ${campaignId}`);
            /* We rather delete a task from MongoDB although we have only paused it because it will be scheduled again if a client presses continue  */
            await this.mongodb.collection('tasks').deleteMany({ 'campaign.id': campaignId });;
            log.verbose('Synchronizer', `Pausing campaignId: ${campaignId} successfully synchronized with MongoDB!`);
            await this.updateCampaignStatus(campaignId, CampaignStatus.PAUSED);
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
            await this.updateCampaignStatus(campaignId, CampaignStatus.SENDING);
        }
    }

    async updateCampaignStatus(campaignId, status) {
        await knex.transaction(async tx => {
            await tx('campaigns').where('id', campaignId).update({ status });
            await activityLog.logEntityActivity('campaign',
                CampaignActivityType.STATUS_CHANGE,
                campaignId,
                { status }
            );
        });
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
            const messageData = queuedMessage.data;
            const collectedMessageData = await this.dataCollector.collectData({
                type: queuedMessage.type,
                campaignId: messageData.campaignId,
                listId: messageData.listId,
                subscriptionId: messageData.subscriptionId,
                to: messageData.to,
                hash_email: messageData.hash_email,
                hashEmailPiece: messageData.hashEmailPiece,
                sendConfigurationId: queuedMessage.send_configuration,
                attachments: messageData.attachments,
                html: messageData.html,
                text: messageData.text,
                subject: messageData.subject,
                tagLanguage: messageData.tagLanguage,
                renderedHtml: messageData.renderedHtml,
                renderedText: messageData.renderedText,
                rssEntry: messageData.rssEntry,
                mergeTags: messageData.mergeTags,
                encryptionKeys: messageData.encryptionKeys
            });

            preparedQueuedMessages.push(collectedMessageData);
        }

        await this.mongodb.collection('queued').insertMany(queuedMessages);
        log.verbose('Synchronizer', 'Queued messages successfully sent to MongoDB!');

        /* Remove all sent messages */
        const deletingIds = scheduledQueuedMessages.map(queuedMessage => queuedMessage.id);
        await knex('queued').whereIn('id', deletingIds).del();
    }

    async synchronizeSendingCampaignsFromMongoDB() {
        await this.synchronizeLinksFromMongoDB();
        await this.synchronizeSentCampaignMessagesFromMongoDB();

        /* Find FINISHED campaigns and remove them from tasks */
        const sendingCampaigns = await this.mongodb.collection('tasks').find({}).limit(CHUNK_SIZE).toArray();
        for (const sendingCampaign of sendingCampaigns) {
            const campaignId = sendingCampaign.campaign.id;
            const remainingCampaignMessages = await this.mongodb.collection('campaign_messages').find({
                campaign: campaignId
            }).limit(CHUNK_SIZE).toArray();

            if (!remainingCampaignMessages.length) {
                log.verbose('Synchronizer', `Campaign with id: ${campaignId} is finished!`);
                await this.mongodb.collection('tasks').deleteMany({ _id: sendingCampaign._id });
                await this.updateCampaignStatus(campaignId, CampaignStatus.FINISHED);
            }
        }
    }

    async synchronizeLinksFromMongoDB() {
        const links = await this.mongodb.collection('links').find({
            status: LinkStatus.UNSYNCHRONIZED
        }).limit(CHUNK_SIZE).toArray();

        if (links.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Received ${links.length} links from MongoDB!`);

        for (const link of links) {
            await this.mongodb.collection('links').updateOne({ _id: link._id }, { $set: { status: LinkStatus.SYNCHRONIZED } });
            delete link._id, link.status;
            await insertIfNotExists(link);
        }
    }

    /* Synchronize all sent campaign messages from MongoDB and do the final processing (update campaign_messages table) */
    async synchronizeSentCampaignMessagesFromMongoDB() {
        const campaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: { $in: [CampaignMessageStatus.SENT, CampaignMessageStatus.FAILED] },
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

        if (campaignMessages.length === 0) {
            return;
        }

        log.verbose('Synchronizer', `Received ${campaignMessages.length} sent messages from MongoDB!`);
        for (const campaignMessage of campaignMessages) {
            if (campaignMessage.status === CampaignMessageStatus.FAILED) {
                await knex('campaign_messages')
                    .where({ id: campaignMessage._id })
                    .update({
                        status: CampaignMessageStatus.FAILED,
                        updated: new Date()
                    });
            } else {
                await knex('campaign_messages')
                    .where({ id: campaignMessage._id })
                    .update({
                        status: CampaignMessageStatus.SENT,
                        response: campaignMessage.response,
                        response_id: campaignMessage.responseId,
                        updated: new Date()
                    });

                await knex('campaigns').where('id', campaignMessage.campaign).increment('delivered');
            }
        }

        const deletingIds = campaignMessages.map(campaignMessage => campaignMessage._id);
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
            if (queuedMessage.type === MessageType.TRIGGERED) {
                await this.processSentTriggeredMessage(queuedMessage)
            }

            if (queuedMessage.campaign && queuedMessage.type === MessageType.TEST) {
                await this.processSentCampaignTestMessage()
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
                hash_email: triggeredMessage.hashEmail,
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
                list: testMessage.listId,
                subscription: testMessage.subscriptionId
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

    process.on('message', msg => {
        if (msg) {
            const type = msg.type;

            if (type === 'schedule-check') {
                /* noinspection JSIgnoredPromiseFromCall */
                synchronizer.callImmediateScheduleCheck();
            }
        }
    });

    if (config.title) {
        process.title = config.title + ': synchronizer';
    }

    process.send({
        type: 'synchronizer-started'
    });
}

/* noinspection JSIgnoredPromiseFromCall */
spawnSynchronizer();
