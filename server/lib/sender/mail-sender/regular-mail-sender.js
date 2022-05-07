'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageErrorType, CampaignMessageStatus } = require('../../../../shared/campaigns');
const { MessageType } = require('../../../../shared/messages');
const { enforce } = require('../../helpers');

/**
 * The class which inherits from MailSender and is responsible for sending mails of REGULAR campaigns.
 */
class RegularMailSender extends MailSender {
    async sendMail(mail, campaignMessageID) {
        // enforce(this.campaign.type === MessageType.REGULAR);
        /*
           We set the campaign_message to SENT before the message is actually sent.
           This is to avoid multiple delivery if by chance we run out of disk space
           and couldn't change status in the database after the message has been sent out
        */
        await this.mongodb.collection('campaign_messages')
            .updateOne({
                _id: campaignMessageID
            }, {
                $set: {
                    status: CampaignMessageStatus.SENT,
                    updated: new Date()
                }
            });

        let result;
        try {
            result = await super.sendMail(mail);
        } catch (err) {
            if (err.campaignMessageErrorType === CampaignMessageErrorType.PERMANENT) {
                await this.mongodb.collection('campaign_messages')
                    .updateOne({
                        _id: campaignMessageID
                    }, {
                        $set: {
                            status: CampaignMessageStatus.FAILED,
                            updated: new Date()
                        }
                    });
            } else {
                await this.mongodb.collection('campaign_messages')
                    .updateOne({
                        _id: campaignMessageID
                    }, {
                        $set: {
                            status: CampaignMessageStatus.SCHEDULED,
                            updated: new Date()
                        }
                    });
            }
            throw err;
        }

        await this.mongodb.collection('campaign_messages')
            .updateOne({
                _id: campaignMessageID
            }, {
                $set: {
                    response: result.response,
                    response_id: result.responseId,
                    updated: new Date()
                }
            });

        /* TODO Synchronize with Mailtrain */
        /*await knex('campaigns').where('id', this.campaign.id).increment('delivered');*/
    }
}

module.exports = RegularMailSender;
