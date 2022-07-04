'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageStatus } = require('../../../../shared/campaigns');

/**
 * The class which inherits from MailSender and is responsible for sending mails of queued not campaign (API_TRANSACTIONAL, SUBSCRIPTION) messages.
 */
class QueuedMailSender extends MailSender {
    async sendMail(mail, queuedMessageId) {
        await this.mongodb.collection('queued')
            .updateOne({
                _id: queuedMessageId
            }, {
                $set: {
                    status: CampaignMessageStatus.SENT,
                    updated: new Date()
                }
            });

        let result;
        try {
            result = await super.sendMail(mail);
        } catch(error) {
            await this.mongodb.collection('queued')
            .updateOne({
                _id: queuedMessageId
            }, {
                $set: {
                    status: CampaignMessageStatus.SCHEDULED,
                    updated: new Date()
                }
            });
            throw error;
        }

        await this.mongodb.collection('queued')
            .updateOne({
                _id: queuedMessageId
            }, {
                $set: {
                    response: result.response,
                    response_id: result.responseId,
                    updated: new Date()
                }
            });
    }
}

module.exports = QueuedMailSender;
