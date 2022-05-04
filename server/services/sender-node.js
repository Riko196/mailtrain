'use strict';

const mongodbInit = require('../lib/mongodb');
const log = require('../lib/log');
const activityLog = require('../lib/activity-log');
const RegularMailMaker = require('../lib/sender/mail-maker/regular-mail-maker');
const RegularMailSender = require('../lib/sender/mail-sender/regular-mail-sender');
const { SendConfigurationError } = require('../lib/sender/mail-sender/mail-sender');

/**
 * The main component of distributed system for making and sending mails.
 */
 class SenderNode {
    async senderNodeLoop() {
        this.mongodb = await mongodbInit();
        try {
            // Make the appropriate DB calls
            setInterval(async () => {
                await this.listTasks();
            }, 5000);
        } catch (e) {
            console.error(e);
        }
    }

    async listTasks(){
        const taskList = await this.mongodb.collection('tasks').find();

        console.log('Tasks:');
        taskList.forEach(task => {
            // console.log(` - ${JSON.stringify(task, null, ' ')}\n\n\n`)
            this.processRegularCampaign(task);
        });
    };

    async processCampaignMessages(campaignData) {
        log.verbose('Sender', 'Start to processing regular campaign ...');

        const regularMailMaker = new RegularMailMaker(campaignData);
        const regularMailSender = new RegularMailSender(campaignData);

        for (const campaignMessage of campaignData.messages) {
            try {
                const mail = await regularMailMaker.makeMail(campaignMessage);
                await regularMailSender.sendMail(mail);
                //await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, campaignId, campaignMessage.list, campaignMessage.subscription);
                log.verbose('Sender', 'Message sent and status updated for %s:%s', campaignMessage.list, campaignMessage.subscription);
            } catch (error) {
                if (error instanceof SendConfigurationError) {
                    log.error('Sender',
                        `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}. Will retry the message if within retention interval.`);
                    break;
                } else {
                    log.error('Sender', `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${error}.`);
                }
                throw error;
            }
        }
    }

    /*async function processQueuedMessages(sendConfigurationId, messages) {
        let withErrors = false;

        for (const queuedMessage of messages) {

            const messageType = queuedMessage.type;

            const msgData = queuedMessage.data;
            let target = '';
            if (msgData.listId && msgData.subscriptionId) {
                target = `${msgData.listId}:${msgData.subscriptionId}`;
            } else if (msgData.to) {
                if (msgData.to.name && msgData.to.address) {
                    target = `${msgData.to.name} <${msgData.to.address}>`;
                } else if (msgData.to.address) {
                    target = msgData.to.address;
                } else {
                    target = msgData.to.toString();
                }
            }

            try {
                await messageSender.sendQueuedMessage(queuedMessage);

                if ((messageType === MessageType.TRIGGERED || messageType === MessageType.TEST) && msgData.campaignId && msgData.listId && msgData.subscriptionId) {
                    await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, msgData.campaignId, msgData.listId, msgData.subscriptionId);
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
    }*/
}

new SenderNode().senderNodeLoop();
