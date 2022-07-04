'use strict';

const MailMaker = require('./mail-maker');

/**
 * The class which inherits from MailSender and is responsible for making mails of queued not campaign (API_TRANSACTIONAL, SUBSCRIPTION) messages.
 */
class QueuedMailMaker extends MailMaker {
    /*
     *  MessageData:
     *      - to ... email / { name, address }
     *      - encryptionKeys [optional]
     *      - mergeTags [used only when campaign / html+text is provided]
     */
    async makeMail(messageData) {
        const mail = { 
            to: messageData.to,
            subject: this.subject,
            headers: messageData.encryptionKeys
        };
        const mergeTags = messageData.mergeTags;
        const message = await this.makeMessage(mergeTags);
        
        return this.accomplishMail(mail, message);
    }

    /* Make string of receiver, used only for debugging purpose. */
    makeTarget(messageData) {
        let target = '';

        if (messageData.to.name && messageData.to.address) {
            target = `${messageData.to.name} <${messageData.to.address}>`;
        } else if (messageData.to.address) {
            target = messageData.to.address;
        } else {
            target = messageData.to.toString();
        }

        return target;
    }
}

module.exports = QueuedMailMaker;
