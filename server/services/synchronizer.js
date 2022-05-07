'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const { CampaignStatus } = require('../../shared/campaigns');
const activityLog = require('../lib/activity-log');
const { CampaignActivityType } = require('../../shared/activity-log');
const log = require('../lib/log');
const DataCollector = require('../lib/sender/synchronizer/data-collector');
const Scheduler = require('../lib/sender/synchronizer/scheduler');

/**
 * The main component for synchronizing between non-high-available centralized and high-available distributed components. It initializes scheduler and
 * data collector and then in loop it communicates with MongoDB database. It takes data from DataCollector and then sends them
 * to MongoDB database at once for ensuring high-availability.
 */
class Synchronizer {
    constructor() {
        log.verbose('Synchronizer', 'Init synchronizer...');
        /* Scheduled campaigns from scheduler for synchronizing */
        this.synchronizingCampaigns = [];
        /* sendConfigurationId -> [queuedMessage] */
        this.sendConfigurationMessageQueue = new Map();
        this.notifier = new Notifier();
        this.dataCollector = new DataCollector();

        /* Connect to the MongoDB and accomplish setup */
        connectToMongoDB().then(() => {
            this.mongodb = getMongoDB();
            this.scheduler = new Scheduler(
                this.synchronizingCampaigns,
                this.sendConfigurationMessageQueue,
                this.notifier
            );
            log.verbose('Synchronizer', this);
            setImmediate(this.synchronizerLoop.bind(this));
        });
    }

    async selectNextTask() {
        return this.synchronizingCampaigns.shift();
    }

    async synchronizerLoop() {
        log.verbose('Synchronizer', 'Starting loop...');

        while (true) {
            const campaignId = await this.selectNextTask();

            if (campaignId) {
                log.verbose('Synchronizer', `New task with campaignId: ${campaignId}`);
                /* TODO implement data collecting also for queue messages */
                const campaignData = await this.dataCollector.collectData({ type: MessageType.REGULAR, campaignId });

                await this.sendDataToMongoDB(campaignData);
                log.verbose('Synchronizer', 'Data successfully sent to MongoDB!');

                await this.updateCampaignStatus(campaignId, CampaignStatus.SENDING);
            } else {
                log.verbose('Synchronizer', 'No task available, I am going to sleep...');
                await this.notifier.waitFor('taskAvailable');
            }
        }
    }

    async sendDataToMongoDB(campaignData) {
        log.verbose('Synchronizer', `Sending data: ${JSON.stringify(campaignData, null, ' ')}`)
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
