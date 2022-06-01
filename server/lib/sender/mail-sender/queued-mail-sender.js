'use strict';

const { MailSender } = require('./mail-sender');
const log = require('../../log');
const { CampaignMessageErrorType, CampaignMessageStatus } = require('../../../../shared/campaigns');
const { MessageType } = require('../../../../shared/messages');
const { enforce } = require('../../helpers');
const { isQueuedMessage } = require('../../../models/queued');

/**
 * The class which inherits from MailSender and is responsible for sending mails of QUEUED messages.
 */
class QueuedMailSender extends MailSender {
    async sendMail(queuedMessage) {
        const messageType = queuedMessage.type;
        enforce(isQueuedMessage(messageType));
        const messageData = queuedMessage.data;

        await knex('queued')
            .where({id: queuedMessage.id})
            .del();

        let result;
        try {
            result = await super.sendMail({
                subscriptionId: messageData.subscriptionId,
                listId: messageData.listId,
                to: messageData.to,
                mergeTags: messageData.mergeTags,
                encryptionKeys: messageData.encryptionKeys
            });
        } catch (error) {
            await knex('queued').insert({
                id: queuedMessage.id,
                send_configuration: queuedMessage.send_configuration,
                type: queuedMessage.type,
                data: JSON.stringify(queuedMessage.data)
            });

            throw error;
        }
    }
}

module.exports = QueuedMailSender;
