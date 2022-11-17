'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { MessageStatus } = require('../../../../shared/messages');

/**
 * The class which inherits from MailSender and is responsible for sending mails of queued not campaign (API_TRANSACTIONAL, SUBSCRIPTION) messages.
 */
class QueuedMailSender extends MailSender {

    /**
     * 
     * @param {*} mail - created mail for the specific subscriber prepared for sending.
     * @param {*} queuedMessageId - ID of a queued message for which mail is sent.
     */
    async sendMail(mail, queuedMessageId) {
        await this.mongodb.collection('queued')
            .updateOne({
                _id: queuedMessageId
            }, {
                $set: {
                    status: MessageStatus.SENT,
                    updated: new Date()
                }
            });

        let result;
        try {
            const isBlacklisted = await this.mongodb.collection('blacklist').findOne({
                email: mail.to
            });

            if (isBlacklisted) {
                result = {};
            } else {
                result = await super.sendMail(mail);
            }
        } catch(error) {
            await this.mongodb.collection('queued')
            .updateOne({
                _id: queuedMessageId
            }, {
                $set: {
                    status: MessageStatus.SCHEDULED,
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
