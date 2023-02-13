'use strict';

const config = require('../../config');
const knex = require('../../knex');
const log = require('../../log');
const activityLog = require('../../activity-log');
const campaigns = require('../../../models/campaigns');
const { CampaignActivityType } = require('../../../../shared/activity-log');
const { CampaignStatus, CampaignType } = require('../../../../shared/campaigns');
const { MessageType, MessageStatus } = require('../../../../shared/messages');

const CHECK_PERIOD = 30 * 1000;
const CHUNK_SIZE = 1000;

/**
 * Scheduler which periodically checks all kinds of campaigns and queued messages and prepares them for next processing.
 */
class Scheduler {
    constructor(synchronizingPausingCampaigns, synchronizingScheduledCampaigns, sendConfigurationMessageQueue, notifier) {
        log.info('Scheduler', 'Init scheduler...');

        this.synchronizingPausingCampaigns = synchronizingPausingCampaigns;
        this.synchronizingScheduledCampaigns = synchronizingScheduledCampaigns;
        this.sendConfigurationMessageQueue = sendConfigurationMessageQueue;
        this.notifier = notifier;
        /* sendConfigurationId -> { retryCount, postponeTill } */
        this.sendConfigurationStatuses = new Map();
        /* Values in seconds which point to the next postpone time when messages fail */
        this.retryBackoff = [100, 150, 200, 300, 600, 600, 1200, 1200, 3000];
        /* Mutexes */
        this.queuedSchedulerRunning = false;
        this.campaignSchedulerRunning = false;
        /* Start periodically schedule every needed campaigns */
        this.periodicCheck();
    }

    /**
     * Method which repeats endlessly with CHECK_PERIOD long pauses and always checks whether there are scheduled campaigns
     * or queued messages and schedules them if yes.
     */
    periodicCheck() {
        /* noinspection JSIgnoredPromiseFromCall */
        log.info('Scheduler', 'Periodic check...');
        this.scheduleCheck();
        setTimeout(this.periodicCheck.bind(this), CHECK_PERIOD);
    }

    /**
     *  One checking round of the Scheduler.
     */
    scheduleCheck() {
        /* This task means synchronizing new data from MongoDB */
        this.notifier.notify('taskAvailable');

        /* noinspection JSIgnoredPromiseFromCall */
        this.scheduleCampaigns();

        /* noinspection JSIgnoredPromiseFromCall */
        this.scheduleQueued();
    }

    /**
     * Schedule queued messages which meet the conditions.
     */
    async scheduleQueued() {
        if (this.queuedSchedulerRunning) {
            return;
        }

        this.queuedSchedulerRunning = true;

        try {
            const sendConfigurationsIdsInProcessing = [...this.sendConfigurationMessageQueue.keys()];
            const postponedSendConfigurationIds = this.getPostponedSendConfigurationIds();

            /* Prune old messages */
            const expirationThresholds = this.getExpirationThresholds();
            for (const type in expirationThresholds) {
                const expirationThreshold = expirationThresholds[type];

                const expiredCount = await knex('queued')
                    .whereNotIn('send_configuration', sendConfigurationsIdsInProcessing)
                    .where('type', type)
                    .where('created', '<', new Date(expirationThreshold.threshold))
                    .del();

                if (expiredCount) {
                    log.warn('Scheduler', `Discarded ${expiredCount} expired ${expirationThreshold.title} message(s).`);
                }
            }

            /* Select queued messages which are not postponed and processing right now by Synchronizer */
            const rows = await knex('queued')
                .whereNotIn('send_configuration', [...sendConfigurationsIdsInProcessing, ...postponedSendConfigurationIds])
                .groupBy('send_configuration')
                .select(['send_configuration']);

            for (const row of rows) {
                const sendConfigurationId = row.send_configuration;
                this.sendConfigurationMessageQueue.set(sendConfigurationId, []);
                /* noinspection JSIgnoredPromiseFromCall */
                await this.prepareQueuedBySendConfiguration(sendConfigurationId);
            }
        } catch (error) {
            log.error('Scheduler', `Scheduling queued messages failed with error: ${error.message}`);
            log.verbose(error.stack);
        }

        this.queuedSchedulerRunning = false;
    }

    /**
     * Prepare scheduled queued messages for Synchronizer.
     * 
     * @argument sendConfigurationId
     */
    async prepareQueuedBySendConfiguration(sendConfigurationId) {
        const msgQueue = this.sendConfigurationMessageQueue.get(sendConfigurationId);

        try {
            const messageIdsInProcessing = [...msgQueue].map(x => x.id);

            /* This orders messages in the following order MessageType.SUBSCRIPTION, MessageType.TEST, MessageType.API_TRANSACTIONAL and MessageType.TRIGGERED */
            const rows = await knex('queued')
                .orderByRaw(`FIELD(type, ${MessageType.TRIGGERED}, ${MessageType.API_TRANSACTIONAL}, ${MessageType.TEST}, ${MessageType.SUBSCRIPTION}) DESC, id ASC`)
                .where('send_configuration', sendConfigurationId)
                .where('status', MessageStatus.SCHEDULED)
                .whereNotIn('id', messageIdsInProcessing)
                .limit(CHUNK_SIZE);

            if (!rows.length) {
                msgQueue.splice(0);
                this.sendConfigurationMessageQueue.delete(sendConfigurationId);
            }

            rows.forEach(row => {
                row.data = JSON.parse(row.data);
                msgQueue.push(row);
            }, this);

            this.notifier.notify('taskAvailable');
        } catch (error) {
            log.error('Scheduler', `Preparing queued messages for send configuration ${sendConfigurationId} failed with error: ${error.message}`);
            log.verbose(error.stack);
        }
    }

    /**
     * Schedule campaigns which meet the conditions.
     */
    async scheduleCampaigns() {
        if (this.campaignSchedulerRunning) {
            return;
        }

        this.campaignSchedulerRunning = true;

        try {
            /* Finish old campaigns */
            const nowDate = new Date();
            const now = nowDate.valueOf();

            const expirationThreshold = new Date(now - config.sender.retention.campaign * 1000);
            const expiredCampaigns = await knex('campaigns')
                .whereIn('campaigns.type', [CampaignType.REGULAR, CampaignType.RSS_ENTRY])
                .whereIn('campaigns.status', [CampaignStatus.SCHEDULED, CampaignStatus.PAUSED])
                .where('campaigns.start_at', '<', expirationThreshold)
                .update({ status: CampaignStatus.FINISHED });

            const pausingCampaigns = await knex('campaigns')
                .whereIn('campaigns.type', [CampaignType.REGULAR, CampaignType.RSS_ENTRY])
                .where('campaigns.status', CampaignStatus.PAUSING)
                .select(['id'])
                .forUpdate();

            pausingCampaigns.forEach(pausingCampaign => {
                const campaignId = pausingCampaign.id;
                this.synchronizingPausingCampaigns.push(campaignId);
            }, this);

            if (pausingCampaigns.length != 0) {
                this.notifier.notify('taskAvailable');
            }

            let campaignId = 0;
            do {
                const postponedSendConfigurationIds = this.getPostponedSendConfigurationIds();

                await knex.transaction(async tx => {
                    const scheduledCampaign = await tx('campaigns')
                        .whereIn('campaigns.type', [CampaignType.REGULAR, CampaignType.RSS_ENTRY])
                        .whereNotIn('campaigns.send_configuration', postponedSendConfigurationIds)
                        .where('campaigns.status', CampaignStatus.SCHEDULED)
                        .where('campaigns.start_at', '<=', nowDate)
                        .select(['id'])
                        .forUpdate()
                        .first();

                    if (scheduledCampaign) {
                        log.verbose('Scheduler', `Scheduled campaign with campaignId: ${scheduledCampaign}`);
                        await tx('campaigns')
                            .where('id', scheduledCampaign.id)
                            .update({ status: CampaignStatus.SYNCHRONIZING });
                        await activityLog.logEntityActivity('campaign',
                            CampaignActivityType.STATUS_CHANGE,
                            scheduledCampaign.id,
                            { status: CampaignStatus.SYNCHRONIZING });
                        campaignId = scheduledCampaign.id;
                    } else {
                        campaignId = 0;
                    }
                });

                if (campaignId) {
                    /* noinspection JSIgnoredPromiseFromCall */
                    await this.prepareCampaign(campaignId);
                } 
            } while (campaignId); 
        } catch (error) {
            log.error('Scheduler', `Scheduling campaigns failed with error: ${error.message}`);
            log.verbose(error.stack);
        }

        this.campaignSchedulerRunning = false;
    }

    /**
     * Prepare scheduled campaign for Synchronizer.
     * 
     * @argument campaignId - Id of campaign for preparing
     */
    async prepareCampaign(campaignId) {
        try {
            await campaigns.prepareCampaignMessages(campaignId);

            const preparedCampaignMessage = await knex('campaign_messages')
                .where({
                    status: MessageStatus.SCHEDULED,
                    campaign: campaignId
                }).first();

            if (!preparedCampaignMessage) {
                await knex('campaigns').where('id', campaignId).update({ status: newStatus });
                await activityLog.logEntityActivity('campaign', CampaignActivityType.STATUS_CHANGE, campaignId, { status: newStatus });
            }

            this.synchronizingScheduledCampaigns.push(campaignId);
            log.verbose('Scheduler', `Notifying synchronizer about campaignId: ${campaignId}`);
            this.notifier.notify('taskAvailable');
        } catch (error) {
            log.error('Scheduler', `Scheduling campaign ${campaignId} failed with error: ${error.message}`);
            log.verbose(error.stack);
        }
    }

    /**
     * @argument sendConfigurationId - Id of sendConfiguration queried for status
     * 
     * @returns sendConfiguration status.
     */
    getSendConfigurationStatus(sendConfigurationId) {
        let status = this.sendConfigurationStatuses[sendConfigurationId];
        if (!status) {
            status = {
                retryCount: 0,
                postponeTill: 0
            };

            this.sendConfigurationStatuses[sendConfigurationId] = status;
        }

        return status;
    }

    /**
     * @returns all postponed sendConfiguration Ids.
     */
    getPostponedSendConfigurationIds() {
        const result = [];
        const now = Date.now();

        for (const [key, value] of Object.entries(this.sendConfigurationStatuses)) {
            if (value.postponeTill > now) {
                result.push(key);
            }
        }

        return result;
    }

    /**
     * Set new retry count for processed messages.
     * 
     * @argument sendConfigurationStatus - status of sendConfiguration needed to be set
     * @argument newRetryCount - order of the next retry count
     */
    setSendConfigurationRetryCount(sendConfigurationStatus, newRetryCount) {
        sendConfigurationStatus.retryCount = newRetryCount;

        let next = 0;
        if (newRetryCount > 0) {
            let backoff;
            if (newRetryCount > this.retryBackoff.length) {
                backoff = this.retryBackoff[retryBackoff.length - 1];
            } else {
                backoff = this.retryBackoff[newRetryCount - 1];
            }

            next = Date.now() + backoff * 1000;
            setTimeout(this.scheduleCheck.bind(this), backoff * 1000);
        }

        sendConfigurationStatus.postponeTill = next;
    }

    /**
     * Method called by Synchronizer when some new chunk of queued messages or campaigns are processed.
     * 
     * @argument sendConfigurationId - Id of sendConfiguration needed to be postponed
     * @argument lastUpdate - timestamp of the last postpone of the given sendConfiguration
     */
    postponeSendConfigurationId(sendConfigurationId, lastUpdate) {
        const sendConfigurationStatus = this.getSendConfigurationStatus(sendConfigurationId);
        if (this.sendConfigurationStatuses[sendConfigurationId].postponeTill <= lastUpdate.getTime()) {
            this.setSendConfigurationRetryCount(sendConfigurationStatus, sendConfigurationStatus.retryCount + 1);
        }
    }

    /**
     * Method called by Synchronizer when some new chunk of queued messages or campaigns are successfully sent and we want to reset SendConfigurationRetryCount.
     * 
     * @argument sendConfigurationId - Id of sendConfiguration needed to reset retry count
     * @argument lastUpdate - timestamp of the last retry count update of the given sendConfiguration
     */
    resetSendConfigurationRetryCount(sendConfigurationId, lastUpdate) {
        if (this.sendConfigurationStatuses[sendConfigurationId] && this.sendConfigurationStatuses[sendConfigurationId].postponeTill <= lastUpdate.getTime()) {
            delete this.sendConfigurationStatuses[sendConfigurationId];
        }
    }

    /**
     * @returns all expiration thresholds for all kind of queued messages which are defined in the main config file.
     */
    getExpirationThresholds() {
        const now = Date.now();

        return {
            [MessageType.TRIGGERED]: {
                threshold: now - config.sender.retention.triggered * 1000,
                title: 'triggered campaign'
            },
            [MessageType.TEST]: {
                threshold: now - config.sender.retention.test * 1000,
                title: 'test campaign'
            },
            [MessageType.SUBSCRIPTION]: {
                threshold: now - config.sender.retention.subscription * 1000,
                title: 'subscription and password-related'
            },
            [MessageType.API_TRANSACTIONAL]: {
                threshold: now - config.sender.retention.apiTransactional * 1000,
                title: 'transactional (API)'
            }
        };
    }
}

module.exports = Scheduler;
