'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const config = require('../lib/config');
const log = require('../lib/log');
const activityLog = require('../lib/activity-log');
const { CampaignTrackerActivityType } = require('../../shared/activity-log');
const RegularMailMaker = require('../lib/sender/mail-maker/regular-mail-maker');
const RegularMailSender = require('../lib/sender/mail-sender/regular-mail-sender');
const QueuedMailMaker = require('../lib/sender/mail-maker/queued-mail-maker');
const QueuedMailSender = require('../lib/sender/mail-sender/queued-mail-sender');
const { isQueuedMessage } = require('../models/queued');
const { SendConfigurationError } = require('../lib/sender/mail-sender/mail-sender');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');
const { MessageType } = require('../../shared/messages');
const { getSubscriptionTableName } = require('../models/subscriptions');

const CHUNK_SIZE = 100;
const WORKERS = config.queue.processes;
const MAX_RANGE = config.queue.maxRange;

const SenderWorkerState = {
    IDLE: 0,
    SENDING: 1,
    DEAD: 2
};

/**
 * The main component of distributed system for making and sending mails.
 */
 class SenderWorker {
    constructor() {
        const myArgs = process.argv.slice(2);
        this.workerId = myArgs[0];
        this.workerState = SenderWorkerState.IDLE;
        this.rangeFrom = Math.floor(MAX_RANGE / WORKERS)  * this.workerId;
        this.rangeTo = Math.floor(MAX_RANGE / WORKERS)  * (this.workerId + 1);

        if (this.rangeTo > MAX_RANGE) {
            this.rangeTo = MAX_RANGE;
        }

        connectToMongoDB().then(() => {
            this.mongodb = getMongoDB();
            this.senderWorkerLoop();
        });
    }

    /* Infinite sender loop which always checks tasks of sending campaigns and queued messages which then sends. */
    async senderWorkerLoop() {
        while (true) {
            try {
                await this.checkCampaignMessages();
                await this.checkQueuedMessages();
            } catch (error) {
                log.error('SenderWorker', error);
            }
        }
    }

    /* Get all subscribers from messages to speed up the whole sending. */
    async getSubscribers(messages) {
        /* listID -> subscribersID */
        const listMap = new Map();
        messages.forEach(message => {
            if (listMap.has(message.list)) {
                listMap.get(message.list).push(message.subscription);
            } else {
                listMap.set(message.list, [message.subscription]);
            }
        });

        /* listID:subscription -> subscriber */
        const subscribers = new Map();
        for (const [key, value] of listMap) {
            const listSubscribers = await this.mongodb.collection(getSubscriptionTableName(key)).find({
                _id: { $in: value }
            }).toArray();

            listSubscribers.forEach(subscriber => {
                subscribers.set(`${key}:${subscriber._id}`, subscriber);
            });
        }

        return subscribers;
    }

    /* Get all blacklisted subscribers from subscribers to speed up the whole sending. */
    async getBlacklisted(subscribers) {
        /* email -> blacklisted */
        const blacklisted = new Map();
        subscribers.forEach((subscriber, key) => {
            blacklisted.set(subscriber.email, false);
        });

        /* Mark all blacklisted subscribers */
        const listBlacklisted = await this.mongodb.collection('blacklist').find({
            email: { $in: Array.from(blacklisted.keys()) }
        }).toArray();

        listBlacklisted.forEach(subscriber => {
            blacklisted.set(subscriber.email, true);
        });

        return blacklisted;
    }

    /* Insert if links not exist in MongoDB which were found during making mails. */
    async insertLinksIfNotExist(links) {
        const queries = [];

        links.forEach(link => {
            const query = {
                updateOne: {
                    filter: { $and: [{ campaign: link.campaign }, { url: link.url }] },
                    update: {
                        $set: {},
                        $setOnInsert: link
                    },
                    upsert: true
                }
            };
            queries.push(query);
        })

        if (queries.length != 0) {
            await this.mongodb.collection('links').bulkWrite(queries, { ordered: false });
        }
    }

    /*
     * Check all tasks of sending campaigns (REGULAR, RSS) and if it is not finished,
     * then send another remaining chunk of mails.
     */
    async checkCampaignMessages(){
        const taskList = await this.mongodb.collection('tasks').find({
            'campaign.status': CampaignStatus.SENDING
        }).toArray();

        //log.verbose('SenderWorker', `Received taskList: ${taskList}`);

        for (const task of taskList) {
            const campaignId = task.campaign.id;
            const chunkCampaignMessages = await this.mongodb.collection('campaign_messages').find({
                campaign: campaignId,
                status: CampaignMessageStatus.SCHEDULED,
                hash_email_uint: { $gte: this.rangeFrom, $lt: this.rangeTo }
            }).limit(CHUNK_SIZE).toArray();

            //log.verbose('SenderWorker', `Received ${chunkCampaignMessages.length} chunkCampaignMessages for campaign: ${campaignId}`);
            await this.processCampaignMessages(task, chunkCampaignMessages);
        };
    };

    /* From chunk of campaign messages make mails and send them to SMTP server. */
    async processCampaignMessages(campaignData, campaignMessages) {
        //log.verbose('SenderWorker', 'Start to processing regular campaign ...');
        const start = new Date();
        const campaignId = campaignData.campaign.id;
        const subscribers = await this.getSubscribers(campaignMessages);
        const blacklisted = await this.getBlacklisted(subscribers);
        const regularMailMaker = new RegularMailMaker(campaignData, subscribers);
        const regularMailSender = new RegularMailSender(
            campaignData.sendConfiguration,
            campaignData.configItems,
            campaignData.isMassEmail,
            blacklisted
        );

        for (const campaignMessage of campaignMessages) {
            try {
                const mail = await regularMailMaker.makeMail(campaignMessage);
                await regularMailSender.sendMail(mail, campaignMessage._id);
                await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, campaignId, campaignMessage.list, campaignMessage.subscription);
                log.verbose('SenderWorker', `Message sent and status updated for ${campaignMessage.list}:${campaignMessage.subscription}`);
            } catch (error) {
                console.log(error);
                if (error instanceof SendConfigurationError) {
                    log.error('SenderWorker',
                        `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}. Will retry the message if within retention interval.`);
                    break;
                } else {
                    log.error('SenderWorker', `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}.`);
                }
            }
        }
        const end = new Date();
        console.log('TIME: ', (end - start) / 1000);

        await this.insertLinksIfNotExist(regularMailMaker.links);
    }

    /*
     * Check queued messages (TRIGGERED, SUBSCRIPTION, TRANSACTIONAL, TEST) and if the queue
     * is not empty then send another remaining chunk of mails.
     */
    async checkQueuedMessages(){
        const chunkQueuedMessages = await this.mongodb.collection('queued').find({
            status: CampaignMessageStatus.SCHEDULED
        }).limit(CHUNK_SIZE).toArray();

        if (chunkQueuedMessages.length !== 0) {
            await this.processQueuedMessages(chunkQueuedMessages);
        }
    };

    /* Get triggered subscriber from triggered (queued) message. */
    async getTriggeredSubscriber(triggeredMessage) {
        const listId = triggeredMessage.listId;
        const subscriptionId = triggeredMessage.subscriptionId;
        /* listID:subscription -> subscriber */
        const triggeredSubscriberMap = new Map();
        const triggeredSubscriber = await this.mongodb.collection(getSubscriptionTableName(listId)).findOne({
            _id: subscriptionId
        });
        triggeredSubscriberMap.set(`${listId}:${subscriptionId}`, triggeredSubscriber);
        return triggeredSubscriberMap;
    }

    /* Return whether queuedMessage is triggered or test message. */
    isTriggeredOrTest(queuedMessage) {
        return (queuedMessage.type === MessageType.TRIGGERED || queuedMessage.type === MessageType.TEST) &&
            queuedMessage.campaign && queuedMessage.listId && queuedMessage.subscriptionId;
    }

    /* From chunk of queued messages make mails and send them to SMTP server. */
    async processQueuedMessages(queuedMessages) {
        log.verbose('SenderWorker', 'Start to processing queued messages ...');
        for (const queuedMessage of queuedMessages) {
            const subscriber = this.isTriggeredOrTest(queuedMessage) ?
                await this.getTriggeredSubscriber(queuedMessage) :
                new Map();

            const blacklisted = this.isTriggeredOrTest(queuedMessage) ?
                await this.getBlacklisted(subscriber) :
                new Map();

            const queuedMailMaker = new QueuedMailMaker(queuedMessage, subscriber);
            const queuedMailSender = new QueuedMailSender(
                queuedMessage.sendConfiguration,
                queuedMessage.configItems,
                queuedMessage.isMassEmail,
                blacklisted
            );
            const target = queuedMailMaker.makeTarget(queuedMessage);

            try {
                const mail = await queuedMailMaker.makeMail(queuedMessage);
                await queuedMailSender.sendMail(mail, queuedMessage._id);
                if (this.isTriggeredOrTest(queuedMessage)) {
                    await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT,
                         queuedMessage.campaign.campaignId, queuedMessage.listId, queuedMessage.subscriptionId);
                }
                log.verbose('SenderWorker', `Message sent and status updated for ${target}`);
            } catch (error) {
                if (error instanceof SendConfigurationError) {
                    log.error('SenderWorker',
                        `Sending message to ${target} failed with error: ${error.message}. Will retry the message if within retention interval.`);
                    withErrors = true;
                    break;
                } else {
                    log.error('SenderWorker',
                        `Sending message to ${target} failed with error: ${error.message}. Dropping the message.`);
                    log.verbose(error.stack);

                    try {
                        //await this.mongodb.collection('queued').deleteOne({ _id: queuedMessage._id });
                    } catch (error) {
                        log.error(error.stack);
                    }
                }
            }

            await this.insertLinksIfNotExist(queuedMailMaker.links);
        }
    }
}

new SenderWorker();
