'use strict';

const MailMaker = require('./mail-maker');

/**
 * The class which inherits from MailSender and is responsible for making mails of REGULAR messages.
 */
class RegularMailMaker extends MailMaker {
    async makeMail(campaignMessage) {
        const subData = {
            listId: campaignMessage.list,
            subscriptionId: campaignMessage.subscription
        };
        return await super.makeMail(subData);
    }
}

module.exports = RegularMailMaker;
