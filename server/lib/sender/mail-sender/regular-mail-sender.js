'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageErrorType, CampaignMessageStatus } = require('../../../../shared/campaigns');

/**
 * The class which inherits from MailSender and is responsible for sending mails of REGULAR campaigns.
 */
class RegularMailSender extends MailSender {
    async sendMail(mail, campaignMessageID) {
        //log.verbose('RegularMailSender', `Starting to sending mail for ${mail.to.address} ...`);
        /*
         * We set the campaign_message to SENT before the message is actually sent.
         * This is to avoid multiple delivery if by chance we run out of disk space
         * and couldn't change status in the database after the message has been sent out
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
                            response: result.response,
                            response_id: result.responseId,
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
    }
}

module.exports = RegularMailSender;
