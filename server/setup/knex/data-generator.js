"use strict";

const faker = require("faker");
const shortid = require("../../lib/shortid");
const { hashEmail, hashToUint32 } = require("../../lib/helpers");
const { MessageType } = require("../../../shared/messages");

function getAdminUser() {
  return {
    id: 1,
    username: "admin",
    password: "$2a$10$sr0xccKJF6zgBGnCQ.Ka5envlBjMDpmIKyF6PCNDYec6FGDy27o/q", // password "test"
    email: "admin@admin.org",
    namespace: 1,
    name: "Administrator",
    role: "master",
  };
}

function getFakeUser() {
  const firstName = faker.name.firstName();
  const lastName = faker.name.lastName();

  return {
    username: firstName,
    password: "$2a$10$sr0xccKJF6zgBGnCQ.Ka5envlBjMDpmIKyF6PCNDYec6FGDy27o/q", // password "test"
    email: faker.internet.email(firstName, lastName),
    namespace: 1,
    name: firstName + " " + lastName,
    role: "master",
  };
}

function getFakeUniqueSubscriber(uniqueID) {
  const email = 'firstName' + uniqueID + '.lastName' + uniqueID + '@fakeGmail.com';
  return {
    cid: uniqueID,
    email: email,
    hash_email: hashEmail(email),
    source_email: -1,
  };
}

function getFakeSubscriber() {
  const firstName = faker.name.firstName();
  const lastName = faker.name.lastName();
  const email = faker.internet.email(firstName, lastName);
  const cid = shortid.generate();

  return {
    cid: cid,
    email: email,
    hash_email: hashEmail(email),
    source_email: -1,
  };
}

function getRandomList() {
  const firstName = faker.name.firstName();
  const cid = shortid.generate();

  return {
    cid: cid,
    name: "List" + firstName,
    description: "Description of list " + cid,
    namespace: 1,
    contact_email: "admin@admin.org",
    send_configuration: 1,
  };
}

function getRandomCampaign() {
  const firstName = faker.name.firstName();
  const cid = shortid.generate();
  const data = {
    sourceCustom: {
      type: "mosaico",
      tag_language: "simple",
      data: { mosaicoTemplate: 1 },
      html: "",
      text: "",
    },
  };
  return {
    cid: cid,
    type: 1,
    name: "Campaign" + firstName,
    description: "Description of campaign" + cid,
    subject: "Subject of campaign " + cid,
    status: 1,
    namespace: 1,
    data: JSON.stringify(data),
    source: 2,
    send_configuration: 1,
  };
}

function getFakeSubscribers() {
  let accounts = 1000 * 1000;
  let row = 0;
  let firstName = faker.name.firstName(); // Rowan Nikolaus
  let lastName = faker.name.lastName(); // Rowan Nikolaus
  let email = faker.internet.email(firstName, lastName); // Kassandra.Haley@erich.biz

  let subscriber = {
    firstName,
    lastName,
    email,
    company: faker.company.companyName(),
    phone: faker.phone.phoneNumber(),
  };

  process.stdout.write(
    "\n" +
      Object.keys(subscriber)
        .map((key) => JSON.stringify(subscriber[key]))
        .join(",")
  );
  if (++row < accounts) {
    setImmediate(getNext);
  }
}

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

async function getFakeTriggeredMessage() {
}

async function getFakeTestMessage() {
  const messageData = {
    campaignId: campaignId,
    subject: 'SUBJECT',
    html: '<!DOCTYPE html> <html> </html>',
    text: 'TEXT',
    tagLanguage: '<!DOCTYPE html> <html> </html>',
    attachments: []
  };
}

function getFakeSubscriptionMessage() {
  const html = '<!DOCTYPE html> <html> </html>';
  const firstName = faker.name.firstName();
  const lastName = faker.name.lastName();
  const to = faker.internet.email(firstName, lastName);
  const hash_email = hashEmail(to);
  const hashEmailPiece = hashToUint32(hash_email);

  return {
    renderedHtml: html,
    renderedText: "  ",
    to,
    hash_email,
    hashEmailPiece,
    subject: 'SUBJECT',
    encryptionKeys: []
  };
}

function getFakeAPITransactionalMessage() {
  const html = '<!DOCTYPE html> <html> </html>';
  const firstName = faker.name.firstName();
  const lastName = faker.name.lastName();
  const email = faker.internet.email(firstName, lastName);
  const hash_email = hashEmail(email);
  const hashEmailPiece = hashToUint32(hash_email);

  return {
      to: {
          address: email
      },
      hash_email,
      hashEmailPiece,
      html,
      text: 'TEXT',
      tagLanguage: html,
      subject: 'SUBJECT',
      mergeTags: { key: 'TAG' },
      attachments: []
  };
}

module.exports.getAdminUser = getAdminUser;
module.exports.getFakeUser = getFakeUser;
module.exports.getFakeUniqueSubscriber = getFakeUniqueSubscriber;
module.exports.getFakeSubscriber = getFakeSubscriber;
module.exports.getRandomList = getRandomList;
module.exports.getRandomCampaign = getRandomCampaign;
module.exports.getFakeSubscribers = getFakeSubscribers;
module.exports.getRandomInt = getRandomInt;
module.exports.getFakeTriggeredMessage = getFakeTriggeredMessage;
module.exports.getFakeTestMessage = getFakeTestMessage;
module.exports.getFakeSubscriptionMessage = getFakeSubscriptionMessage;
module.exports.getFakeAPITransactionalMessage = getFakeAPITransactionalMessage;
