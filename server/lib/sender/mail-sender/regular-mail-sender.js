'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageErrorType } = require('../../../../shared/campaigns');
const { MessageType } = require('../../../../shared/messages');
const { enforce } = require('../../helpers');

/**
 * The class which inherits from MailSender and is responsible for sending mails of REGULAR campaigns.
 */
class RegularMailSender extends MailSender {
    async sendMail(mail) {
        // enforce(this.campaign.type === MessageType.REGULAR);

        // We set the campaign_message to SENT before the message is actually sent. This is to avoid multiple delivery
        // if by chance we run out of disk space and couldn't change status in the database after the message has been sent out
        /*await knex('campaign_messages')
            .where({id: campaignMessage.id})
            .update({
                status: CampaignMessageStatus.SENT,
                updated: new Date()
            });*/

        let result;
        try {
            result = await super.sendMail(mail);
            console.log('RESULT: ', result);
        } catch (err) {
            if (err.campaignMessageErrorType === CampaignMessageErrorType.PERMANENT) {
                /*await knex('campaign_messages')
                    .where({id: campaignMessage.id})
                    .update({
                        status: CampaignMessageStatus.FAILED,
                        updated: new Date()
                    });*/
            } else {
                /*await knex('campaign_messages')
                    .where({id: campaignMessage.id})
                    .update({
                        status: CampaignMessageStatus.SCHEDULED,
                        updated: new Date()
                    });*/
            }
            throw err;
        }

        /*await knex('campaign_messages')
            .where({id: campaignMessage.id})
            .update({
                response: result.response,
                response_id: result.responseId,
                updated: new Date()
            });

        await knex('campaigns').where('id', this.campaign.id).increment('delivered');*/
    }
}

module.exports = RegularMailSender;
