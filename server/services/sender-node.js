'use strict';

const { connectToMongoDB, getMongoDB } = require('../lib/mongodb');
const log = require('../lib/log');
const activityLog = require('../lib/activity-log');
const RegularMailMaker = require('../lib/sender/mail-maker/regular-mail-maker');
const RegularMailSender = require('../lib/sender/mail-sender/regular-mail-sender');
const QueuedMailMaker = require('../lib/sender/mail-maker/queued-mail-maker');
const QueuedMailSender = require('../lib/sender/mail-sender/queued-mail-sender');
const { SendConfigurationError } = require('../lib/sender/mail-sender/mail-sender');
const { CampaignStatus, CampaignMessageStatus } = require('../../shared/campaigns');

const CHUNK_SIZE = 100;

/**
 * The main component of distributed system for making and sending mails.
 */
 class SenderNode {
    async senderNodeLoop() {
        await connectToMongoDB();
        this.mongodb = getMongoDB();
        while (true) {
            try {
                await this.checkCampaignMessages();
                //await this.checkQueuedMessages();
            } catch (error) {
                console.error(error);
            }
        }
    }

    async checkCampaignMessages(){
        const taskList = await this.mongodb.collection('tasks').find({
            'campaign.status': CampaignStatus.SENDING
        }).toArray();

        //log.verbose('Sender', `Received taskList: ${taskList}`);

        for (const task of taskList) {
            const campaignId = task.campaign.id;
            const chunkCampaignMessages = await this.mongodb.collection('campaign_messages').find({
                campaign: campaignId,
                status: CampaignMessageStatus.SCHEDULED
            }).limit(CHUNK_SIZE).toArray();

            //log.verbose('Sender', `Received ${chunkCampaignMessages.length} chunkCampaignMessages for campaign: ${campaignId}`);

            if (chunkCampaignMessages.length === 0) {
                await this.mongodb.collection('tasks')
                    .updateOne({
                        _id: task._id
                    }, {
                        $set: {
                            status: CampaignStatus.FINISHED,
                            updated: new Date()
                        }
                    });
            } else {
                await this.processCampaignMessages(task, chunkCampaignMessages);
            }
        };
    };

    async checkQueuedMessages(){
        const chunkQueuedMessages = await this.mongodb.collection('queued').find({
            status: CampaignMessageStatus.SCHEDULED
        }).limit(CHUNK_SIZE).toArray();
    };

    async processCampaignMessages(campaignData, campaignMessages) {
        //log.verbose('Sender', 'Start to processing regular campaign ...');
        let counter = 0;
        const campaignId = campaignData.campaign.id;
        const regularMailMaker = new RegularMailMaker(campaignData);
        const regularMailSender = new RegularMailSender(campaignData);
        const startTime = new Date();
        for (const campaignMessage of campaignMessages) {
            try {
                const mail = await regularMailMaker.makeMail(campaignMessage);
                await regularMailSender.sendMail(mail, campaignMessage._id);
                //await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, campaignId, campaignMessage.list, campaignMessage.subscription);
                //log.verbose('Sender', `Message ${counter} sent and status updated for ${campaignMessage.list}:${campaignMessage.subscription}`);
                counter += 1;
            } catch (error) {
                console.log(error);
                if (error instanceof SendConfigurationError) {
                    log.error('Sender',
                        `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}. Will retry the message if within retention interval.`);
                    break;
                } else {
                    log.error('Sender', `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}.`);
                }
            }
        }
        const endTime = new Date();
        console.log('TIME: ', (endTime - startTime)/1000);
    }

    async processQueuedMessages(queuedMessages) {
        log.verbose('Sender', 'Start to processing queued messages ...');
        for (const queuedMessage of queuedMessages) {
            const messageData = queuedMessage.data;
            const queuedMailMaker = new QueuedMailMaker(messageData);
            const queuedMailSender = new QueuedMailSender(messageData);
            const target = queuedMailMaker.makeTarget(messageData);

            try {
                const mail = await queuedMailMaker.makeMail(queue)
                await messageSender.sendQueuedMessage(queuedMessage);

                if ((messageType === MessageType.TRIGGERED || messageType === MessageType.TEST) && messageData.campaignId && messageData.listId && messageData.subscriptionId) {
                    await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, messageData.campaignId, messageData.listId, messageData.subscriptionId);
                }

                log.verbose('Senders', `Message sent and status updated for ${target}`);
            } catch (err) {
                if (err instanceof mailers.SendConfigurationError) {
                    log.error('Senders', `Sending message to ${target} failed with error: ${err.message}. Will retry the message if within retention interval.`);
                    withErrors = true;
                    break;
                } else {
                    log.error('Senders', `Sending message to ${target} failed with error: ${err.message}. Dropping the message.`);
                    log.verbose(err.stack);

                    try {
                        await messageSender.dropQueuedMessage(queuedMessage);
                    } catch (err) {
                        log.error(err.stack);
                    }
                }
            }
        }
    }
}

new SenderNode().senderNodeLoop();
