'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');
const activityLog = require('../lib/activity-log');
const { CampaignActivityType } = require('../../shared/activity-log');
const log = require('../lib/log');
const DataCollector = require('../lib/sender/synchronizer/data-collector');
const Scheduler = require('../lib/sender/synchronizer/scheduler');

const CHUNK_SIZE = 1000;

/**
 * The main component for synchronizing between non-high-available centralized Mailtrain and high-available distributed Sender
 * and vice versa. It initializes Scheduler and DataCollector and then in loop it communicates with MongoDB database. It takes
 * data from DataCollector and then sends them to MongoDB database at once for ensuring high-availability.
 */
class Synchronizer {
    constructor() {
        log.verbose('Synchronizer', 'Init synchronizer...');
        /* Scheduled operations (Pause, Reset, Continue) from scheduler for synchronizing */
        this.synchronizingOperations = [];
        /* Scheduled campaigns from scheduler for synchronizing */
        this.synchronizingCampaigns = [];
        /* sendConfigurationId -> [queuedMessage] */
        this.synchronizingQueuedMessages = new Map();
        this.notifier = new Notifier();
        this.dataCollector = new DataCollector();

        /* Connect to the MongoDB and accomplish setup */
        connectToMongoDB().then(() => {
            this.mongodb = getMongoDB();
            this.scheduler = new Scheduler(
                this.synchronizingCampaigns,
                this.synchronizingQueuedMessages,
                this.notifier
            );
            log.verbose('Synchronizer', this);
            setImmediate(this.synchronizerLoop.bind(this));
        });
    }

    async synchronizerLoop() {
        log.verbose('Synchronizer', 'Starting loop...');

        while (true) {
            log.verbose('Synchronizer', 'Starting synchronizing...');
            try {
                /* Mailtrain --> MongoDB */
                await this.synchronizeScheduledOperations();
                /* Mailtrain --> MongoDB */
                await this.synchronizeScheduledCampaigns();
                /* Mailtrain --> MongoDB */
                //await this.synchronizeScheduledQueuedMessages();
                /* MongoDB --> Mailtrain */
                await this.synchronizeSentCampaignMessagesFromMongoDB();
                /* MongoDB --> Mailtrain */
                //await this.synchronizeSentQueuedMessagesFromMongoDB();
            } catch(error) {
                log.error('Synchronizer', `Synchronizing failed with error: ${error.message}`);
                log.verbose(error.stack);
            }

            await this.notifier.waitFor('taskAvailable');
        }
    }
    async selectScheduledOperation() {
        return this.synchronizingOperations.shift();
    }

    async synchronizeScheduledOperations() {
    }

    async selectScheduledCampaign() {
        return this.synchronizingCampaigns.shift();
    }

    async synchronizeScheduledCampaigns() {
        const campaignId = await this.selectScheduledCampaign();

        if (campaignId) {
            log.verbose('Synchronizer', `New task with campaignId: ${campaignId}`);
            /* Collect all needed campaign data for sending */
            const campaignData = await this.dataCollector.collectData({
                type: MessageType.REGULAR,
                campaignId
            });

            await this.sendCampaignToMongoDB(campaignData);
            log.verbose('Synchronizer', 'Data successfully sent to MongoDB!');
            await this.updateCampaignStatus(campaignId, CampaignStatus.SENDING);
        }
    }

    async sendCampaignToMongoDB(campaignData) {
        //log.verbose('Synchronizer', `Sending data: ${JSON.stringify(campaignData, null, ' ')}`)
        this.mongodb.collection('tasks').insertOne(campaignData);
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

    async selectScheduledQueuedMessages() {
        return this.synchronizingCampaigns.shift();
    }

    async synchronizeScheduledQueuedMessages() {
        const scheduledQueuedMessages = await this.selectScheduledQueuedMessages();

        if (scheduledQueuedMessages) {
            log.verbose('Synchronizer', `New task with queued messages: ${scheduledQueuedMessages}`);
            const preparedQueuedMessages = [];

            /* Collect all needed for each queued message for sending */
            for (const queuedMessage of queuedMessages) {
                const messageData = queuedMessage.data;

                const collectedMessageData = await this.dataCollector.collectData({
                    type: queuedMessage.type,
                    campaignId: messageData.campaignId,
                    listId: messageData.listId,
                    sendConfigurationId: queuedMessage.send_configuration,
                    attachments: messageData.attachments,
                    html: messageData.html,
                    text: messageData.text,
                    subject: messageData.subject,
                    tagLanguage: messageData.tagLanguage,
                    renderedHtml: messageData.renderedHtml,
                    renderedText: messageData.renderedText,
                    rssEntry: messageData.rssEntry
                });

                preparedQueuedMessages.push(collectedMessageData);
            }

            await this.sendQueuedMessagesToMongoDB(preparedQueuedMessages);
            log.verbose('Synchronizer', 'Data successfully sent to MongoDB!');
        }
    }

    async sendQueuedMesagesToMongoDB(queuedMessages) {
        //log.verbose('Synchronizer', `Sending queued messages: ${JSON.stringify(campaignData, null, ' ')}`)
        this.mongodb.collection('queued').insertMany(queuedMessages);
    }

    async synchronizeSentCampaignMessagesFromMongoDB() {
        log.verbose('Synchronizer', 'Synchronizing sent campaign messages from MongoDB...');

        const campaignMessages = await this.mongodb.collection('campaign_messages').find({
            status: { $in: [CampaignMessageStatus.SENT, CampaignMessageStatus.FAILED] },
            response: { $ne: null }
        }).limit(CHUNK_SIZE).toArray();

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
                        response: campaignMessage.response,
                        response_id: campaignMessage.responseId,
                        updated: new Date()
                    });

                await knex('campaigns').where('id', campaignMessage.campaign).increment('delivered');
            }
        }

        const deletingIds = campaignMessages.map(campaignMessage => campaignMessage._id);
        await this.mongodb.collection('campaign_messages').deleteMany({ id: { $in: deletingIds } });
    }

    async synchronizeSentQueuedMessagesFromMongoDB() {
        log.verbose('Synchronizer', 'Synchronizing sent queued messages from MongoDB...');
        const queuedMessages = await this.mongodb.collection('queued').find({
            status: { $in: [CampaignMessageStatus.SENT] }
        }).limit(CHUNK_SIZE).toArray();

        for (const queuedMessage of queuedMessages) {
            if (messageType === MessageType.TRIGGERED) {
                await this.processSentTriggeredMessage()
            }

            if (campaign && messageType === MessageType.TEST) {
                await this.processSentCampaignTestMessage()
            }

            if (msgData.attachments) {
                await this.unlockAttachments(msgData.attachments);
            }
        }

        const deletingIds = queuedMessages.map(queuedMessage => queuedMessage._id);
        await this.mongodb.collection('queued').deleteMany({ id: { $in: deletingIds } });
    }

    async processSentTriggeredMessage() {
        await knex('campaign_messages').insert({
            hash_email: result.subscriptionGrouped.hash_email,
            subscription: result.subscriptionGrouped.id,
            campaign: campaign.id,
            list: result.list.id,
            send_configuration: queuedMessage.send_configuration,
            status: CampaignMessageStatus.SENT,
            response: result.response,
            response_id: result.response_id,
            updated: new Date()
        });

        await knex('campaigns').where('id', campaign.id).increment('delivered');
    }

    async processSentCampaignTestMessage() {
        try {
            /*
             * Insert an entry to test_messages. This allows us to remember test sends to lists that are not
             * listed in the campaign - see the check in getMessage
             */
            await knex('test_messages').insert({
                campaign: campaign.id,
                list: result.list.id,
                subscription: result.subscriptionGrouped.id
            });
        } catch (error) {
            if (error.code === 'ER_DUP_ENTRY') {
                /* The entry is already there, so we can ignore this error */
            } else {
                throw error;
            }
        }
    }

    async unlockAttachments(attachments) {
        for (const attachment of attachments) {
            /* This means that it is an attachment recorded in table files_campaign_attachment */
            if (attachment.id) {
                try {
                    /* We ignore any errors here because we already sent the message. Thus we have to mark it as completed to avoid sending it again.*/
                    await knex.transaction(async tx => {
                        await files.unlockTx(tx, 'campaign', 'attachment', attachment.id);
                    });
                } catch (err) {
                    log.error('MessageSender', `Error when unlocking attachment ${attachment.id} for ${result.email} (queuedId: ${queuedMessage.id})`);
                    log.verbose(err.stack);
                }
            }
        }
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

new Synchronizer();
