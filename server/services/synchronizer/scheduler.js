const knex = require('../../lib/knex');
const log = require('../../lib/log');
const campaigns = require('../../models/campaigns');
const { CampaignStatus, CampaignType, CampaignActivityType } = require('../../../shared/campaigns');

const WorkAssignmentType = {
    CAMPAIGN: 0,
    QUEUED: 1
};

/**
 * Scheduler which periodically checks all kinds of campaigns and prepare them for next processing.
 */
class Scheduler {
    this.CHECK_PERIOD = 30 * 1000;

    constructor(synchronizingCampaigns) {
        this.synchronizingCampaigns = synchronizingCampaigns;
        this.queuedSchedulerRunning = false;
        this.campaignSchedulerRunning = false;

        /* Start periodically schedule every needed campaigns */
        this.periodicCheck();
    }

    periodicCheck() {
        /* noinspection JSIgnoredPromiseFromCall */
        this.scheduleCheck();

        setTimeout(this.periodicCheck, this.CHECK_PERIOD);
    }

    scheduleCheck() {
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
            const sendConfigurationsIdsInProcessing = [...sendConfigurationMessageQueue.keys()];
            const postponedSendConfigurationIds = getPostponedSendConfigurationIds();

            // prune old messages
            const expirationThresholds = getExpirationThresholds();
            for (const type in expirationThresholds) {
                const expirationThreshold = expirationThresholds[type];

                const expiredCount = await knex('queued')
                    .whereNotIn('send_configuration', sendConfigurationsIdsInProcessing)
                    .where('type', type)
                    .where('created', '<', new Date(expirationThreshold.threshold))
                    .del();

                if (expiredCount) {
                    log.warn('Senders', `Discarded ${expiredCount} expired ${expirationThreshold.title} message(s).`);
                }
            }

            const rows = await knex('queued')
                .whereNotIn('send_configuration', [...sendConfigurationsIdsInProcessing, ...postponedSendConfigurationIds])
                .groupBy('send_configuration')
                .select(['send_configuration']);

            for (const row of rows) {
                const sendConfigurationId = row.send_configuration;
                sendConfigurationMessageQueue.set(sendConfigurationId, []);

                // noinspection JSIgnoredPromiseFromCall
                this.prepareQueuedBySendConfiguration(sendConfigurationId);
            }
        } catch (err) {
            log.error('Senders', `Scheduling queued messages failed with error: ${err.message}`);
            log.verbose(err.stack);
        }

        this.queuedSchedulerRunning = false;
    }

    async prepareQueuedBySendConfiguration(sendConfigurationId) {
        const msgQueue = sendConfigurationMessageQueue.get(sendConfigurationId);

        function isCompleted()  {
            /* TODO add condition whether all messages have been sent */
            if (preparedCampaignMessages.length > 0)
                return false;
            else
                return true;
        }

        async function finish(deleteMsgQueue) {
            /* TODO some synchronize operation with worker ? */
            /* while (!isCompleted()) {
                await notifier.waitFor('workerFinished');
            } */
            if (deleteMsgQueue) {
                sendConfigurationMessageQueue.delete(sendConfigurationId);
            }
        }


        try {
            while (true) {
                if (isSendConfigurationPostponed(sendConfigurationId)) {
                    return await finish(true, true);
                }

                let messagesInProcessing = [...msgQueue];
                for (const wa of workAssignment.values()) {
                    if (wa.type === WorkAssignmentType.QUEUED && wa.sendConfigurationId === sendConfigurationId) {
                        messagesInProcessing = messagesInProcessing.concat(wa.messages);
                    }
                }

                const messageIdsInProcessing = messagesInProcessing.map(x => x.id);

                const rows = await knex('queued')
                    .orderByRaw(`FIELD(type, ${MessageType.TRIGGERED}, ${MessageType.API_TRANSACTIONAL}, ${MessageType.TEST}, ${MessageType.SUBSCRIPTION}) DESC, id ASC`) // This orders messages in the following order MessageType.SUBSCRIPTION, MessageType.TEST, MessageType.API_TRANSACTIONAL and MessageType.TRIGGERED
                    .where('send_configuration', sendConfigurationId)
                    .whereNotIn('id', messageIdsInProcessing)
                    .limit(retrieveBatchSize);

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
                        msgQueue.push(row);
                    }
                }

                for (const type in expirationThresholds) {
                    const expirationThreshold = expirationThresholds[type];
                    if (expirationCounters[type] > 0) {
                        log.warn('Senders', `Discarded ${expirationCounters[type]} expired ${expirationThreshold.title} message(s).`);
                    }
                }

                notifier.notify('workAvailable');
            }
        } catch (err) {
            log.error('Senders', `Sending queued messages for send configuration ${sendConfigurationId} failed with error: ${err.message}`);
            log.verbose(err.stack);
        }
    }

    async scheduleCampaigns() {
        if (this.campaignSchedulerRunning) {
            return;
        }

        this.campaignSchedulerRunning = true;

        try {
            // Finish old campaigns
            const nowDate = new Date();
            const now = nowDate.valueOf();

            const expirationThreshold = new Date(now - config.queue.retention.campaign * 1000);
            const expiredCampaigns = await knex('campaigns')
                .whereIn('campaigns.type', [CampaignType.REGULAR, CampaignType.RSS_ENTRY])
                .whereIn('campaigns.status', [CampaignStatus.SCHEDULED, CampaignStatus.PAUSED])
                .where('campaigns.start_at', '<', expirationThreshold)
                .update({ status: CampaignStatus.FINISHED });

            /* TODO ensure pausing campaign */
            const pausingCampaigns = await knex('campaigns')
                .whereIn('campaigns.type', [CampaignType.REGULAR, CampaignType.RSS_ENTRY])
                .where('campaigns.status', CampaignStatus.PAUSING)
                .select(['id'])
                .forUpdate();

            while (true) {
                let campaignId = 0;
                const postponedSendConfigurationIds = getPostponedSendConfigurationIds();

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
                        await tx('campaigns')
                            .where('id', scheduledCampaign.id)
                            .update({ status: CampaignStatus.SYNCHRONIZING});
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
            log.error('Senders', `Scheduling campaigns failed with error: ${err.message}`);
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
            /* TODO some synchronize operation with worker ? */
            /* while (!isCompleted()) {
                await notifier.waitFor('workerFinished');
            } */

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

                sendConfigurationIdByCampaignId.set(campaign.id, campaign.send_configuration);

                if (this.isSendConfigurationPostponed(campaign.send_configuration)) {
                    /* Postpone campaign if its send configuration is problematic */
                    return await finish(CampaignStatus.SCHEDULED);
                }

                /* TODO optimize this operation */
                preparedCampaignMessages = await knex('campaign_messages')
                    .where({ status: CampaignMessageStatus.SCHEDULED, campaign: campaignId })
                    .whereNotIn('hash_email', messagesInProcessing.map(x => x.hash_email));

                if (preparedCampaignMessages.length === 0) {
                    if (isCompleted()) {
                        return await finish(false, CampaignStatus.FINISHED);
                    } else {
                        /* TODO Maybe some additional synchronize operation */
                        /* At this point, there might be messages that re-appeared because sending failed. */
                        continue;
                    }
                }

                this.synchronizingCampaigns.push(campaignId);
            }
        } catch (err) {
            log.error('Senders', `Scheduling campaign ${campaignId} failed with error: ${err.message}`);
            log.verbose(err.stack);
        }
    }

    isSendConfigurationPostponed(sendConfigurationId) {
        const now = Date.now();
        const sendConfigurationStatus = this.getSendConfigurationStatus(sendConfigurationId);
        return sendConfigurationStatus.postponeTill > now;
    }

    getSendConfigurationStatus(sendConfigurationId) {
        let status = sendConfigurationStatuses.get(sendConfigurationId);
        if (!status) {
            status = {
                retryCount: 0,
                postponeTill: 0
            };

            sendConfigurationStatuses.set(sendConfigurationId, status);
        }

        return status;
    }

    getPostponedSendConfigurationIds() {
        const result = [];
        const now = Date.now();

        for (const entry of sendConfigurationStatuses.entries()) {
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