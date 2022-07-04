'use strict';

const knex = require('../lib/knex');
const { MessageType } = require('../../shared/messages');
const tools = require('../lib/tools');
const htmlToText = require('html-to-text');
const files = require('./files');
const fields = require('./fields');
const subscriptions = require('./subscriptions');
const contextHelpers = require('../lib/context-helpers');
const { enforce, hashEmail } = require('../lib/helpers');
const sender = require('../lib/sender/sender');
const DataCollector = require('../lib/sender/synchronizer/data-collector');
const CampaignMailMaker = require('../lib/sender/mail-maker/campaign-mail-maker');

async function queueCampaignMessageTx(tx, sendConfigurationId, listId, subscriptionId, messageType, messageData) {
    enforce(messageType === MessageType.TRIGGERED || messageType === MessageType.TEST);

    const msgData = { ...messageData };

    if (msgData.attachments) {
        for (const attachment of messageData.attachments) {
            await files.lockTx(tx,'campaign', 'attachment', attachment.id);
        }
    }

    msgData.listId = listId;
    msgData.subscriptionId = subscriptionId;

    const subscriptionGrouped = await subscriptions.getById(contextHelpers.getAdminContext(), listId, subscriptionId);
    msgData.hash_email = hashEmail(subscriptionGrouped.email);
    
    await tx('queued').insert({
        send_configuration: sendConfigurationId,
        type: messageType,
        data: JSON.stringify(msgData)
    });
}

async function queueSubscriptionMessage(sendConfigurationId, to, subject, encryptionKeys, template) {
    let html, text;

    const htmlRenderer = await tools.getTemplate(template.html, template.locale);
    if (htmlRenderer) {
        html = htmlRenderer(template.data || {});

        if (html) {
            html = await tools.prepareHtml(html);
        }
    }

    const textRenderer = await tools.getTemplate(template.text, template.locale);
    if (textRenderer) {
        text = textRenderer(template.data || {});
    } else if (html) {
        text = htmlToText.fromString(html, {
            wordwrap: 130
        });
    }

    const msgData = {
        renderedHtml: html,
        renderedText: text,
        to,
        hash_email: hashEmail(to),
        subject,
        encryptionKeys
    };

    await knex('queued').insert({
        send_configuration: sendConfigurationId,
        type: MessageType.SUBSCRIPTION,
        data: JSON.stringify(msgData)
    });

    sender.scheduleCheck();
}

async function queueAPITransactionalMessageTx(tx, sendConfigurationId, email, subject, html, text, tagLanguage, mergeTags, attachments) {
    const msgData = {
        to: {
            address: email
        },
        hash_email: hashEmail(email),
        html,
        text,
        tagLanguage,
        subject,
        mergeTags,
        attachments
    };

    await tx('queued').insert({
        send_configuration: sendConfigurationId,
        type: MessageType.API_TRANSACTIONAL,
        data: JSON.stringify(msgData)
    });
}

async function dropQueuedMessage(queuedMessage) {
    await knex('queued')
        .where({id: queuedMessage.id})
        .del();
}

/* TODO transfer it to different module */
async function getArchivedMessage(campaignCid, listCid, subscriptionCid, settings, isTest = false) {
    const dataCollector = new DataCollector();

    const campaignData = await dataCollector.collectData({
        type: MessageType.REGULAR,
        campaignCid,
        listCid,
        ...settings
    });

    const campaignMailMaker = new CampaignMailMaker(campaignData);

    const campaign = campaignMailMaker.campaign;
    const list = campaignMailMaker.listsByCid[listCid];

    const subscriptionGrouped = await subscriptions.getByCid(contextHelpers.getAdminContext(), list.id, subscriptionCid, true, isTest);

    let listOk = false;

    for (const listSpec of campaign.lists) {
        if (list.id === listSpec.list) {
            // This means we send to a list that is associated with the campaign
            listOk = true;
            break;
        }
    }

    if (!listOk) {
        const row = await knex('test_messages').where({
            campaign: campaign.id,
            list: list.id,
            subscription: subscriptionGrouped.id
        }).first();

        if (row) {
            listOk = true;
        }
    }

    if (!listOk) {
        throw new Error('Message not found');
    }

    const flds = campaignMailMaker.listsFieldsGrouped[list.id];
    const mergeTags = fields.getMergeTags(flds, subscriptionGrouped, campaignMailMaker.getExtraTags());

    return await campaignMailMaker.makeMessage(mergeTags, list, subscriptionGrouped, false);
}

function isQueuedMessage(messageType) {
    return (messageType === MessageType.TRIGGERED || messageType === MessageType.TEST || messageType === MessageType.SUBSCRIPTION || messageType === MessageType.API_TRANSACTIONAL);
}

module.exports.queueCampaignMessageTx = queueCampaignMessageTx;
module.exports.queueSubscriptionMessage = queueSubscriptionMessage;
module.exports.queueAPITransactionalMessageTx = queueAPITransactionalMessageTx;
module.exports.dropQueuedMessage = dropQueuedMessage;
module.exports.getArchivedMessage = getArchivedMessage;
module.exports.isQueuedMessage = isQueuedMessage;
