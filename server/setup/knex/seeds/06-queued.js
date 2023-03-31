'use strict';
const {
    getRandomInt,
    getFakeTriggeredMessage,
    getFakeTestMessage,
    getFakeSubscriptionMessage,
    getFakeAPITransactionalMessage
} = require('../data-generator');
const { MessageType, MessageStatus } = require("../../../../shared/messages");

exports.seed = (knex, Promise) => (async() => {
  const countOfMessages = 5000;
  for (let i = 1; i <= countOfMessages; i++) {
    const randomInt = getRandomInt(4);
    let message;
    if (randomInt === 0) {
        message = getFakeTriggeredMessage();
    } else if (randomInt === 1) {
        message = getFakeTestMessage();
    } else if (randomInt === 2) {
        message = getFakeSubscriptionMessage();

        await knex('queued').insert({
            send_configuration: 1,
            type: MessageType.SUBSCRIPTION,
            data: JSON.stringify(message),
            status: MessageStatus.SCHEDULED
        });
    } else {
        message = getFakeAPITransactionalMessage();

        await knex('queued').insert({
            send_configuration: 1,
            type: MessageType.API_TRANSACTIONAL,
            data: JSON.stringify(message),
            status: MessageStatus.SCHEDULED
        });
    }
  }
})();
