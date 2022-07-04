'use strict';

const config = require('../../config');
const knex = require('../../knex');
const log = require('../../log');
const activityLog = require('../../activity-log');
const campaigns = require('../../../models/campaigns');
const { CampaignActivityType } = require('../../../../shared/activity-log');
const { CampaignStatus, CampaignMessageStatus, CampaignType } = require('../../../../shared/campaigns');
const { MessageType } = require('../../../../shared/messages');

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
        /* sendConfigurationId -> {retryCount, postponeTill} */
        this.sendConfigurationStatuses = new Map();
        /* campaignId -> sendConfigurationId */
        this.sendConfigurationIdByCampaignId = new Map();
        /* Mutexes */
        this.queuedSchedulerRunning = false;
        this.campaignSchedulerRunning = false;
        /* Start periodically schedule every needed campaigns */
        this.periodicCheck();
    }

    periodicCheck() {
        /* noinspection JSIgnoredPromiseFromCall */
        log.info('Scheduler', 'Periodic check...');
        this.scheduleCheck();
        setTimeout(this.periodicCheck.bind(this), CHECK_PERIOD);
    }

    scheduleCheck() {
        /* This task means synchronizing new data from MongoDB */
        this.notifier.notify('taskAvailable');

        /* noinspection JSIgnoredPromiseFromCall */
        this.scheduleCampaigns();

        /* noinspection JSIgnoredPromiseFromCall */
        this.scheduleQueued();
    }

    async scheduleQueued() {
        if (this.queuedSchedulerRunning) {
            return;
        }

        this.queuedSchedulerRunning = true;

        try {
            const sendConfigurationsIdsInProcessing = [...this.sendConfigurationMessageQueue.keys()];
            const postponedSendConfigurationIds = this.getPostponedSendConfigurationIds();

            // prune old messages
            const expirationThresholds = this.getExpirationThresholds();
            for (const type in expirationThresholds) {
                const expirationThreshold = expirationThresholds[type];

                const expiredCount = await knex('queued')
                    .whereNotIn('send_configuration', sendConfigurationsIdsInProcessing)
                    .where('type', type)
                    .where('created', '<', new Date(expirationThreshold.threshold))
                    .del();

                if (expiredCount) {
                    log.warn('Sender', `Discarded ${expiredCount} expired ${expirationThreshold.title} message(s).`);
                }
            }

            const rows = await knex('queued')
                .whereNotIn('send_configuration', [...sendConfigurationsIdsInProcessing, ...postponedSendConfigurationIds])
                .groupBy('send_configuration')
                .select(['send_configuration']);

            for (const row of rows) {
                const sendConfigurationId = row.send_configuration;
                this.sendConfigurationMessageQueue.set(sendConfigurationId, []);

                // noinspection JSIgnoredPromiseFromCall
                this.prepareQueuedBySendConfiguration(sendConfigurationId);
            }
        } catch (err) {
            log.error('Sender', `Scheduling queued messages failed with error: ${err.message}`);
            log.verbose(err.stack);
        }

        this.queuedSchedulerRunning = false;
    }

    async prepareQueuedBySendConfiguration(sendConfigurationId) {
        const msgQueue = this.sendConfigurationMessageQueue.get(sendConfigurationId);

        function isCompleted() {
            return msgQueue.length > 0;
        }

        async function finish(clearMsgQueue, deleteMsgQueue) {
            if (clearMsgQueue) {
                msgQueue.splice(0);
            }

            if (deleteMsgQueue) {
                sendConfigurationMessageQueue.delete(sendConfigurationId);
            }
        }

        try {
            while (true) {
                if (this.isSendConfigurationPostponed(sendConfigurationId)) {
                    return await finish(true, true);
                }

                const messageIdsInProcessing = [...msgQueue].map(x => x.id);

                /* This orders messages in the following order MessageType.SUBSCRIPTION, MessageType.TEST, MessageType.API_TRANSACTIONAL and MessageType.TRIGGERED */
                const rows = await knex('queued')
                    .orderByRaw(`FIELD(type, ${MessageType.TRIGGERED}, ${MessageType.API_TRANSACTIONAL}, ${MessageType.TEST}, ${MessageType.SUBSCRIPTION}) DESC, id ASC`)
                    .where('send_configuration', sendConfigurationId)
                    .whereNotIn('id', messageIdsInProcessing)
                    .limit(CHUNK_SIZE);

                if (rows.length === 0) {
                    if (isCompleted()) {
                        return await finish(false, true);
                    } else {
                        await finish(false, false);
                        // At this point, there might be new messages in the queued that could belong to us. Thus we have to try again instead for returning.
                        continue;
                    }
                }

                const expirationThresholds = this.getExpirationThresholds();
                const expirationCounters = {};
                for (const type in expirationThresholds) {
                    expirationCounters[type] = 0;
                }

                for (const row of rows) {
                    const expirationThreshold = expirationThresholds[row.type];

                    if (row.created < expirationThreshold.threshold) {
                        expirationCounters[row.type] += 1;
                        await knex('queued').where('id', row.id).del();
                    } else {
                        row.data = JSON.parse(row.data);
                        log.verbose('Scheduler', `Scheduled new queued messages: ${msgQueue}`);
                        msgQueue.push(row);
                    }
                }

                for (const type in expirationThresholds) {
                    const expirationThreshold = expirationThresholds[type];
                    if (expirationCounters[type] > 0) {
                        log.warn('Sender', `Discarded ${expirationCounters[type]} expired ${expirationThreshold.title} message(s).`);
                    }
                }

                this.notifier.notify('taskAvailable');
                return;
            }
        } catch (err) {
            log.error('Sender', `Sending queued messages for send configuration ${sendConfigurationId} failed with error: ${err.message}`);
            log.verbose(err.stack);
        }
    }

    async scheduleCampaigns() {
        if (this.campaignSchedulerRunning) {
            return;
        }

        this.campaignSchedulerRunning = true;

        try {
            /* Finish old campaigns */
            const nowDate = new Date();
            const now = nowDate.valueOf();

            const expirationThreshold = new Date(now - config.queue.retention.campaign * 1000);
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

            for (const pausingCampaign of pausingCampaigns) {
                const campaignId = pausingCampaign.id;
                this.synchronizingPausingCampaigns.push(campaignId);
            }

            if (pausingCampaigns.length != 0) {
                this.notifier.notify('taskAvailable');
            }

            while (true) {
                let campaignId = 0;
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
                    }
                });

                if (campaignId) {
                    /* noinspection JSIgnoredPromiseFromCall */
                    this.prepareCampaign(campaignId);
                } else {
                    break;
                }
            }
        } catch (err) {
            log.error('Sender', `Scheduling campaigns failed with error: ${err.message}`);
            log.verbose(err.stack);
        }

        this.campaignSchedulerRunning = false;
    }

    async prepareCampaign(campaignId) {
        let preparedCampaignMessages = [];

        function isCompleted()  {
            /* TODO add condition whether all messages have been sent */
            if (preparedCampaignMessages.length > 0)
                return false;
            else
                return true;
        }

        async function finish(newStatus) {
            if (newStatus) {
                await knex('campaigns').where('id', campaignId).update({ status: newStatus });
                await activityLog.logEntityActivity('campaign', CampaignActivityType.STATUS_CHANGE, campaignId, { status: newStatus });
            }
        }

        try {
            await campaigns.prepareCampaignMessages(campaignId);

            while (true) {
                const campaign = await knex('campaigns').where('id', campaignId).first();

                if (campaign.status === CampaignStatus.PAUSING) {
                    return await finish(CampaignStatus.PAUSED);
                }

                const expirationThreshold = Date.now() - config.queue.retention.campaign * 1000;
                if (campaign.start_at && campaign.start_at.valueOf() < expirationThreshold) {
                    return await finish(CampaignStatus.FINISHED);
                }

                this.sendConfigurationIdByCampaignId.set(campaign.id, campaign.send_configuration);

                if (this.isSendConfigurationPostponed(campaign.send_configuration)) {
                    /* Postpone campaign if its send configuration is problematic */
                    return await finish(CampaignStatus.SCHEDULED);
                }

                const preparedCampaignMessage = await knex('campaign_messages')
                    .where({
                        status: CampaignMessageStatus.SCHEDULED,
                        campaign: campaignId
                    }).first();

                if (!preparedCampaignMessage) {
                    if (isCompleted()) {
                        return await finish(false, CampaignStatus.FINISHED);
                    } else {
                        /* TODO Maybe some additional synchronize operation */
                        /* At this point, there might be messages that re-appeared because sending failed. */
                        continue;
                    }
                }

                this.synchronizingScheduledCampaigns.push(campaignId);
                log.verbose('Scheduler', `Notifying synchronizer about campaignId: ${campaignId}`);
                this.notifier.notify('taskAvailable');
                return;
            }
        } catch (err) {
            log.error('Sender', `Scheduling campaign ${campaignId} failed with error: ${err.message}`);
            log.verbose(err.stack);
        }
    }

    isSendConfigurationPostponed(sendConfigurationId) {
        const now = Date.now();
        const sendConfigurationStatus = this.getSendConfigurationStatus(sendConfigurationId);
        return sendConfigurationStatus.postponeTill > now;
    }

    getSendConfigurationStatus(sendConfigurationId) {
        let status = this.sendConfigurationStatuses.get(sendConfigurationId);
        if (!status) {
            status = {
                retryCount: 0,
                postponeTill: 0
            };

            this.sendConfigurationStatuses.set(sendConfigurationId, status);
        }

        return status;
    }

    getPostponedSendConfigurationIds() {
        const result = [];
        const now = Date.now();

        for (const entry of this.sendConfigurationStatuses.entries()) {
            if (entry[1].postponeTill > now) {
                result.push(entry[0]);
            }
        }

        return result;
    }

    getExpirationThresholds() {
        const now = Date.now();

        return {
            [MessageType.TRIGGERED]: {
                threshold: now - config.queue.retention.triggered * 1000,
                title: 'triggered campaign'
            },
            [MessageType.TEST]: {
                threshold: now - config.queue.retention.test * 1000,
                title: 'test campaign'
            },
            [MessageType.SUBSCRIPTION]: {
                threshold: now - config.queue.retention.subscription * 1000,
                title: 'subscription and password-related'
            },
            [MessageType.API_TRANSACTIONAL]: {
                threshold: now - config.queue.retention.apiTransactional * 1000,
                title: 'transactional (API)'
            }
        };
    }
}

module.exports = Scheduler;
