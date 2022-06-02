'use strict';

const MailMaker = require('./mail-maker');

/**
 * The class which inherits from MailSender and is responsible for making mails of QUEUED messages.
 */
class QueuedMailMaker extends MailMaker {
    async makeMail(messageData) {
        const subData = {
            subscriptionId: messageData.subscriptionId,
            listId: messageData.listId,
            to: messageData.to,
            mergeTags: messageData.mergeTags,
            encryptionKeys: messageData.encryptionKeys
        };
        return await super.makeMail(subData);
    }

    /* Make string of receiver, used only for debugging purpose. */
    makeTarget(messageData) {
        let target = '';

        if (messageData.listId && messageData.subscriptionId) {
            target = `${messageData.listId}:${messageData.subscriptionId}`;
        } else if (messageData.to) {
            if (messageData.to.name && messageData.to.address) {
                target = `${messageData.to.name} <${messageData.to.address}>`;
            } else if (messageData.to.address) {
                target = messageData.to.address;
            } else {
                target = messageData.to.toString();
            }
        }

        return target;
    }
}

module.exports = QueuedMailMaker;
