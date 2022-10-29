'use strict';

const { connectToMongoDB, getMongoDB, getNewTransactionSession } = require('../lib/mongodb');
const config = require('../lib/config');
const log = require('../lib/log');
const activityLog = require('../lib/activity-log');
const { sleep } = require('../lib/helpers');
const { CampaignTrackerActivityType } = require('../../shared/activity-log');
const CampaignMailMaker = require('../lib/sender/mail-maker/campaign-mail-maker');
const CampaignMailSender = require('../lib/sender/mail-sender/campaign-mail-sender');
const QueuedMailMaker = require('../lib/sender/mail-maker/queued-mail-maker');
const QueuedMailSender = require('../lib/sender/mail-sender/queued-mail-sender');
const PlatformSolver = require('../lib/sender/sender-worker/platform-solver');
const { SenderWorkerState, senderWorkerInit, senderWorkerSynchronizedInit } = require('../lib/sender/sender-worker/init');
const WorkerSynchronizer = require('../lib/sender/sender-worker/worker-synchronizer');
const { SendConfigurationError } = require('../lib/sender/mail-sender/mail-sender');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');
const { MessageType } = require('../../shared/messages');
const subscriptions = require('../models/subscriptions');

/** Chunk of messages which will be processed in one iteration. */
const CHUNK_SIZE = 100;

/**
 * The main component of distributed system for making and sending mails.
 */
 class SenderWorker {
    constructor(senderWorkerInfo) {
        /* Get number of maximum workers who can be spawned */
        this.maxWorkers = senderWorkerInfo.maxWorkers;
        /* Get WorkerID */
        this.workerId = senderWorkerInfo._id;
        /* Get the current worker state (used as a reference object) */
        this.state = { value: senderWorkerInfo.state };
        /* List of all ranges that this worker currently owns (his own range + ranges from non-working workers) */
        this.ranges = [senderWorkerInfo.range];
        /* The value which says whether a worker should stop after accomplishing currently executing iteration */
        this.stopWorking = false;
        /* MongoDB session */
        this.mongodb = getMongoDB();
        /* If worker synchronization is set, start an asynchronous callback that creates WorkerSynchronizer
         * for this worker and takes care of worker synchronization */
        if (PlatformSolver.workerSynchronizationIsSet()) {
            this.workerSynchronizer = new WorkerSynchronizer(senderWorkerInfo, this.ranges);
        }
        log.info(`SenderWorker:${this.workerId}`, `${JSON.stringify(this.ranges, null, 4)}`);
        /* Start doing sender worker loop */
        setImmediate(this.senderWorkerLoop.bind(this));
    }

    /* Wait until your substitute finish if you are substituted and then update yourself to WORKING state. */
    async waitAndPrepareForTheStart() {
        let preparedWorker = null;
        while (!preparedWorker) {
            preparedWorker = await this.mongodb.collection('sender_workers').findOne({
                _id: this.workerId,
                substitute: null
            });
            
            /* Wait 5s until the next check */
            if (!preparedWorker) {
                log.info(`SenderWorker:${this.workerId}`, `I am still substituted, I have to wait...`);
                await sleep(5000);
                await this.workerSynchronizer.solvePotentialDeadlock();
            }
        }

        /* Set yourself to WORKING state */
        await this.mongodb.collection('sender_workers').updateOne(
            { _id: this.workerId },
            { $set: { state: SenderWorkerState.WORKING } }
        );
        this.state.value = SenderWorkerState.WORKING;

    }

    /* Infinite sender loop which always checks tasks of sending campaigns and queued messages which then sends. */
    async senderWorkerLoop() {
        if (PlatformSolver.workerSynchronizationIsSet()) {
            /* Wait until you can start and then start worker loop (state === WORKING and substitute === null) */
            await this.waitAndPrepareForTheStart();
        }

        log.info(`SenderWorker:${this.workerId}`, `I am going to work...`);
        while (!this.stopWorking) {
            const transactionSession = getNewTransactionSession();
            try {
                for (const range of this.ranges) {
                    await this.checkCampaignMessages(range);
                    await this.checkQueuedMessages(range);
                }

                if (PlatformSolver.workerSynchronizationIsSet()) {
                    await this.workerSynchronizer.checkSynchronizingWorkers(transactionSession);
                    await this.workerSynchronizer.releaseRedundantSubstitutions(transactionSession);
                }
            } catch (error) {
                log.error(`SenderWorker:${this.workerId}`, error);
                log.error(`SenderWorker:${this.workerId}`, error.stack);
            } finally {
                await transactionSession.endSession();
            }
        }

        log.info(`SenderWorker:${this.workerId}`, 'Killed after successfully completed work!');
        process.exit(0);
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
            const listSubscribers = await this.mongodb.collection(subscriptions.getSubscriptionTableName(key)).find({
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
        /* Get all blacklisted subscribers from this chunk */
        const listBlacklisted = await this.mongodb.collection('blacklist').find({
            email: { $in: Array.from(subscribers.values()).map(subscriber => subscriber.email) }
        }).toArray();

        return listBlacklisted.map(blacklisted => blacklisted.email);
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
    async checkCampaignMessages(range){
        const taskList = await this.mongodb.collection('tasks').find({
            'campaign.status': CampaignStatus.SENDING
        }).toArray();

        // log.verbose(`SenderWorker:${this.workerId}`, `Received taskList: ${taskList}`);

        for (const task of taskList) {
            /* Skip tasks with non-working send configuration */
            if (task.withErrors) {
                continue;
            }
            
            const campaignId = task.campaign.id;
            const chunkCampaignMessages = await this.mongodb.collection('campaign_messages').find({
                campaign: campaignId,
                status: CampaignMessageStatus.SCHEDULED,
                hashEmailPiece: { $gte: range.from, $lt: range.to }
            }).limit(CHUNK_SIZE).toArray();

            /*log.verbose(`SenderWorker:${this.workerId}`,
                `Received ${chunkCampaignMessages.length} chunkCampaignMessages for campaign: ${campaignId}`);*/
            if (chunkCampaignMessages.length) {
                await this.processCampaignMessages(task, chunkCampaignMessages);
            }
        };
    };

    /* From chunk of campaign messages make mails and send them to SMTP server. */
    async processCampaignMessages(campaignData, campaignMessages) {
        //log.verbose(`SenderWorker:${this.workerId}`, 'Start to processing chunk of campaign messages ...');
        const campaignId = campaignData.campaign.id;
        const subscribers = await this.getSubscribers(campaignMessages);
        const blacklisted = await this.getBlacklisted(subscribers);
        const campaignMailMaker = new CampaignMailMaker(campaignData, subscribers);
        const campaignMailSender = new CampaignMailSender(
            campaignData.sendConfiguration,
            campaignData.configItems,
            campaignData.isMassEmail,
            blacklisted
        );

        for (const campaignMessage of campaignMessages) {
            try {
                const mail = await campaignMailMaker.makeMail(campaignMessage);
                const campaignMessageType = campaignMessage.type ? campaignMessage.type : MessageType.REGULAR;
                await campaignMailSender.sendMail(mail, campaignMessageType, campaignMessage._id);
                await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT,
                    campaignId, campaignMessage.list, campaignMessage.subscription);
                /*log.verbose(`SenderWorker:${this.workerId}`,
                    `Message sent and status updated for ${campaignMessage.list}:${campaignMessage.subscription}`);*/
            } catch (error) {
                if (error instanceof SendConfigurationError) {
                    log.error(`SenderWorker:${this.workerId}`,
                        `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}. ` +
                        'Will retry the message if within retention interval.');
                    await this.mongodb.collection('tasks').updateOne({ _id: campaignData._id }, { withErrors: true });
                    break;
                } else {
                    log.error(`SenderWorker:${this.workerId}`,
                        `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}.`);
                }
            }
        }

        await this.insertLinksIfNotExist(campaignMailMaker.links);
    }

    /*
     * Check queued messages (TRIGGERED, SUBSCRIPTION, TRANSACTIONAL, TEST) and if the queue
     * is not empty then send another remaining chunk of mails.
     */
    async checkQueuedMessages(range){
        /* Processing queued campaign messages (TRIGGERED, TEST) */
        const chunkQueuedCampaignMessages = await this.mongodb.collection('queued').find({
            status: CampaignMessageStatus.SCHEDULED,
            type: { $in: [MessageType.TRIGGERED, MessageType.TEST] },
            hashEmailPiece: { $gte: range.from, $lt: range.to }
        }).limit(CHUNK_SIZE).toArray();

        for (const queuedCampaignMessage of chunkQueuedCampaignMessages) {
            await this.processCampaignMessages(queuedCampaignMessage, chunkQueuedCampaignMessages);
        }


        /* Processing queued not campaign messages (API_TRANSACTIONAL, SUBSCRIPTION) */
        const chunkQueuedMessages = await this.mongodb.collection('queued').find({
            status: CampaignMessageStatus.SCHEDULED,
            type: { $in: [MessageType.API_TRANSACTIONAL, MessageType.SUBSCRIPTION] },
            hashEmailPiece: { $gte: range.from, $lt: range.to }
        }).limit(CHUNK_SIZE).toArray();

        if (chunkQueuedMessages.length !== 0) {
            await this.processQueuedMessages(chunkQueuedMessages);
        }
    };

    /* From chunk of queued messages make mails and send them to SMTP server. */
    async processQueuedMessages(queuedMessages) {
        log.verbose(`SenderWorker:${this.workerId}`, 'Start to processing queued messages ...');
        for (const queuedMessage of queuedMessages) {
            const queuedMailMaker = new QueuedMailMaker(queuedMessage);
            const queuedMailSender = new QueuedMailSender(
                queuedMessage.sendConfiguration,
                queuedMessage.configItems,
                queuedMessage.isMassEmail
            );
            const target = queuedMailMaker.makeTarget(queuedMessage);

            try {
                const mail = await queuedMailMaker.makeMail(queuedMessage);
                await queuedMailSender.sendMail(mail, queuedMessage._id);
                log.verbose(`SenderWorker:${this.workerId}`, `Message sent and status updated for ${target}`);
            } catch (error) {
                if (error instanceof SendConfigurationError) {
                    log.error(`SenderWorker:${this.workerId}`,
                        `Sending message to ${target} failed with error: ${error.message}. ` +
                        'Will retry the message if within retention interval.');
                    await this.mongodb.collection('queued').updateOne({ _id: queuedMessage._id }, { withErrors: true });
                    break;
                } else {
                    log.error(`SenderWorker:${this.workerId}`,
                        `Sending message to ${target} failed with error: ${error.message}. Dropping the message.`);
                    log.error(error.stack);

                    try {
                        await this.mongodb.collection('queued').deleteOne({ _id: queuedMessage._id });
                    } catch (error) {
                        log.error(error.stack);
                    }
                }
            }

            await this.insertLinksIfNotExist(queuedMailMaker.links);
        }
    }
}

/* The method which is called as first when the worker process is spawned by mailtrain. */
async function spawnSenderWorker() {
    /* Connect to the MongoDB and accomplish setup */
    await connectToMongoDB();
    /* Get number of all workers (it is taken from different variable according to running platform) */
    const maxWorkers = PlatformSolver.getNumberOfWorkers();
    /* Get worker ID */
    const workerId = PlatformSolver.getWorkerId();
    /* Init SenderWorker and get all info about him */
    const senderWorkerInfo = PlatformSolver.workerSynchronizationIsSet()
        ? await senderWorkerSynchronizedInit(workerId, maxWorkers)
        : senderWorkerInit(workerId, maxWorkers);
    /* Create instance and start working */
    const senderWorker = new SenderWorker(senderWorkerInfo);

    /* Catch Ctrl+C from parent process */
    process.on('SIGINT', () => { senderWorker.stopWorking = true; }); 
    /* Catch kill process from parent process */
    process.on('SIGTERM', () => { senderWorker.stopWorking = true; }); 
    /* Catch kill message from Mailtrain root process */
    process.on('message', msg => {
        if (msg === 'exit') {
            senderWorker.stopWorking = true;
        }
    });

    if (config.title) {
        process.title = config.title + ': sender-worker ' + senderWorker.workerId;
    }

    if (PlatformSolver.isCentralized()) {
        process.send({ type: 'worker-started' });
    }
}

/* noinspection JSIgnoredPromiseFromCall */
spawnSenderWorker();
