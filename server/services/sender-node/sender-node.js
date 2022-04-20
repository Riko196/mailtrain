const mongodb = require('../../lib/mongodb');
const log = require('../../lib/log');
const activityLog = require('../../lib/activity-log');
const MailMaker = require('../../lib/sender/message-sender');

/**
 * The main component of distributed system for sending email.
 */
 class SenderNode {
    constructor() {
    }

    async senderNodeLoop() {
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
        const taskList = await mongodb.collection('tasks');

        console.log('Tasks:');
        taskList.forEach(task => console.log(` - ${task}\n\n\n`));
    };

    async processCampaignMessages(campaignId, messages) {
        const mailMaker = new MailMaker();
        await mailMaker.initByCampaignId(campaignId);

        let withErrors = false;

        for (const campaignMessage of messages) {
            try {
                await cs.sendRegularCampaignMessage(campaignMessage);

                await activityLog.logCampaignTrackerActivity(CampaignTrackerActivityType.SENT, campaignId, campaignMessage.list, campaignMessage.subscription);

                log.verbose('Senders', 'Message sent and status updated for %s:%s', campaignMessage.list, campaignMessage.subscription);
            } catch (err) {

                if (err instanceof mailers.SendConfigurationError) {
                    log.error('Senders', `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${err.message}. Will retry the message if within retention interval.`);
                    withErrors = true;
                    break;
                } else {
                    log.error('Senders', `Sending message to ${campaignMessage.list}:${campaignMessage.subscription} failed with error: ${err.message}.`);

                    log.verbose(err.code);
                    log.verbose(err.response);
                    log.verbose(err.responseCode);
                    log.verbose(err.stack);
                }
            }
        }

    }
}

new SenderNode().senderNodeLoop();
