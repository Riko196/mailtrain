'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageErrorType, CampaignMessageStatus } = require('../../../../shared/campaigns');
const { MessageType } = require('../../../../shared/messages');

/**
 * The class which inherits from MailSender and is responsible for sending mails of campaign (Regular, RSS, Triggered, Test) messages.
 */
class CampaignMailSender extends MailSender {
    constructor(sendConfiguration, configItems, isMassMail, blacklisted) {
        super(sendConfiguration, configItems, isMassMail);
        /* email -> blacklisted */
        this.blacklisted = blacklisted;
    }

    /**
     * The main method called by SenderWorker for sending created mail.
     * 
     * @param {*} mail - created mail for the specific subscriber prepared for sending.
     * @param {*} type - type of message which is sent through mail (REGULAR, TRIGGERED, TEST).
     * @param {*} campaignMessageID - ID of a campaign for which mail is sent.
     */
    async sendMail(mail, type, campaignMessageID) {
        const collectionName = type === MessageType.REGULAR ? 'campaign_messages' : 'queued';
        log.verbose('CampaignMailSender', `Starting to sending mail for ${mail.to.address} ...`);
        /*
         * We set the campaign_message to SENT before the message is actually sent.
         * This is to avoid multiple delivery if by chance we run out of disk space
         * and couldn't change status in the database after the message has been sent out
         */
        await this.mongodb.collection(collectionName)
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
            if (this.blacklisted.get(mail.to)) {
                result = {};
            } else {
                result = await super.sendMail(mail);
            }
        } catch (err) {
            if (err.campaignMessageErrorType === CampaignMessageErrorType.PERMANENT) {
                await this.mongodb.collection(collectionName)
                    .updateOne({
                        _id: campaignMessageID
                    }, {
                        $set: {
                            status: CampaignMessageStatus.FAILED,
                            updated: new Date()
                        }
                    });
            } else {
                await this.mongodb.collection(collectionName)
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

        await this.mongodb.collection(collectionName)
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

module.exports = CampaignMailSender;
