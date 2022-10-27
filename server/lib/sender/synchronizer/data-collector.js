'use strict';

const config = require('../../config');
const campaigns = require('../../../models/campaigns');
const sendConfigurations = require('../../../models/send-configurations');
const lists = require('../../../models/lists');
const fields = require('../../../models/fields');
const files = require('../../../models/files');
const templates = require('../../../models/templates');
const settings = require('../../../models/settings');
const contextHelpers = require('../../context-helpers');
const knex = require('../../knex');
const log = require('../../log');
const { enforce } = require('../../helpers');
const { MessageType } = require('../../../../shared/messages');
const { CampaignSource, CampaignMessageStatus, CampaignStatus } = require('../../../../shared/campaigns');

/**
 * DataCollector collects all needed data for processing (sending) one specific campaign or queued message from MySQL
 * centralized database. Used by Synchronizer.
 */
class DataCollector {
    async collectData(query) {
        // log.verbose('DataCollector', `Starting collecting data for campaign with ID ${query.campaignId}`);
        /* Result data */
        this.data = { withErrors: false };

        const type = query.type;
        await knex.transaction(async tx => {
            /* if is campaign message */
            if (this.isCampaignMessage(type) || this.isQueuedCampaignMessage(type)) {
                this.data.isMassMail = true;

                await this.collectCampaign(tx, query);
                await this.collectSendConfiguration(tx, query);
                await this.collectLists(tx, query);

                /* We have to convert Maps to JSON string in order to be able to send it to MongoDB */
                this.data.listsById = JSON.stringify(this.data.listsById);
                this.data.listsByCid = JSON.stringify(this.data.listsByCid);
                this.data.listsFieldsGrouped = JSON.stringify(this.data.listsFieldsGrouped);
            /* if is queued not campaign message */
            } else if (this.isQueuedNotCampaignMessage(type)) {
                this.data.isMassMail = false;

                await this.collectRSSSendConfiguration(tx, query);
            } else {
                enforce(false);
            }

            await this.collectEmailAttachments(tx, query);
            await this.collectEmailTemplate(tx, query);
        });
        await this.collectSetting(query);

        /* Only for queued messages */
        if (this.isQueuedMessage(type)) {
            this.collectAdditionalQueuedData(query);
        } else {
            this.data.campaign.status = CampaignStatus.SENDING;
        }

        return this.data;
    }

    async collectCampaign(tx, query) {
        if (query.campaign) {
            this.data.campaign = query.campaign;
        } else if (query.campaignCid) {
            this.data.campaign = await campaigns.rawGetByTx(tx, 'cid', query.campaignCid);
        } else if (query.campaignId) {
            this.data.campaign = await campaigns.rawGetByTx(tx, 'id', query.campaignId);
        }

        // log.verbose('DataCollector', `Collected campaign data: ${JSON.stringify(this.data.campaign, null, ' ')}`);
    }

    async collectSendConfiguration(tx, query) {
        if (query.sendConfigurationId) {
            this.data.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(),
                query.sendConfigurationId, false, true);
        } else if (this.data.campaign && this.data.campaign.send_configuration) {
            this.data.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(),
                this.data.campaign.send_configuration, false, true);
        } else {
            enforce(false);
        }

        this.data.useVerp = config.verp.enabled && datasendConfiguration.verp_hostname;
        this.data.useVerpSenderHeader = this.data.useVerp && !this.data.sendConfiguration.verp_disable_sender_header;
        // log.verbose('DataCollector', `Collected sendConfiguration data: ${JSON.stringify(this.data.sendConfiguration, null, ' ')}`);
    }

    async collectRSSSendConfiguration(tx, query) {
        this.data.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(), query.sendConfigurationId, false, true);
        // log.verbose('DataCollector', `Collected RSS sendConfiguration data: ${JSON.stringify(this.data.sendConfiguration, null, ' ')}`);
    }

    async collectLists(tx, query) {
        const listsById = new Map(); // listId -> list
        const listsByCid = new Map(); // listCid -> list
        const listsFieldsGrouped = new Map(); // listId -> fieldsGrouped

        const addList = async (list) => {
            listsById[list.id] = list;
            listsByCid[list.cid] = list;
            listsFieldsGrouped[list.id] = await fields.listGroupedTx(tx, list.id);
        }

        /* These IFs are not mutually exclusive because there are situations when listId is provided, but we want to collect all lists of the campaign
           in order to support tags like LINK_PUBLIC_SUBSCRIBE, LIST_ID_<index>, PUBLIC_LIST_ID_<index> */
        if (query.listId) {
            const list = await lists.getByIdTx(tx, contextHelpers.getAdminContext(), query.listId);
            await addList(list);
        }

        if (query.listCid && !listsByCid.has(query.listCid)) {
            const list = await lists.getByCidTx(tx, contextHelpers.getAdminContext(), query.listCid);
            await addList(list);
        }

        if (this.data.campaign && this.data.campaign.lists) {
            for (const listSpec of this.data.campaign.lists) {
                if (!listsById.has(listSpec.list)) {
                    const list = await lists.getByIdTx(tx, contextHelpers.getAdminContext(), listSpec.list);
                    await addList(list);
                }
            }
        }

        this.data.listsById = listsById;
        this.data.listsByCid = listsByCid;
        this.data.listsFieldsGrouped = listsFieldsGrouped;
    }

    async collectEmailAttachments(tx, query) {
        if (query.attachments) {
            this.data.attachments = query.attachments;
        } else if (this.data.campaign && this.data.campaign.id) {
            const attachments = await files.listTx(tx, contextHelpers.getAdminContext(), 'campaign', 'attachment',
                this.data.campaign.id);

            this.data.attachments = [];
            for (const attachment of attachments) {
                this.data.attachments.push({
                    filename: attachment.originalname,
                    path: files.getFilePath('campaign', 'attachment', this.data.campaign.id, attachment.filename)
                });
            }
        } else {
            this.data.attachments = [];
        }

        // log.verbose('DataCollector', `Collected attachments data: ${JSON.stringify(this.data.attachments, null, ' ')}`);
    }

    async collectEmailTemplate(tx, query) {
        if (query.renderedHtml !== undefined) {
            this.data.renderedHtml = query.renderedHtml;
            this.data.renderedText = query.renderedText;
        } else if (query.html !== undefined) {
            this.data.html = query.html;
            this.data.text = query.text;
            this.data.tagLanguage = query.tagLanguage;
        } else if (this.data.campaign && this.data.campaign.source === CampaignSource.TEMPLATE) {
            this.data.template = await templates.getByIdTx(tx, contextHelpers.getAdminContext(),
                this.data.campaign.data.sourceTemplate, false);
            this.data.html = this.data.template.html;
            this.data.text = this.data.template.text;
            this.data.tagLanguage = this.data.template.tag_language;
        } else if (this.data.campaign &&
            (this.data.campaign.source === CampaignSource.CUSTOM || this.data.campaign.source
                === CampaignSource.CUSTOM_FROM_TEMPLATE || this.data.campaign.source === CampaignSource.CUSTOM_FROM_CAMPAIGN)) {
            this.data.html = this.data.campaign.data.sourceCustom.html;
            this.data.text = this.data.campaign.data.sourceCustom.text;
            this.data.tagLanguage = this.data.campaign.data.sourceCustom.tag_language;
        }

        enforce(this.data.renderedHtml || (this.data.campaign && this.data.campaign.source === CampaignSource.URL)
            || this.data.tagLanguage);

        // log.verbose('DataCollector', `Collected template data: ${JSON.stringify(this.data.template, null, ' ')}`);
    }

    async collectSetting(query) {
        if (query.rssEntry !== undefined) {
            this.data.rssEntry = query.rssEntry;
        } else if (this.data.campaign && this.data.campaign.data.rssEntry) {
            this.data.rssEntry = this.data.campaign.data.rssEntry;
        }

        if (query.subject !== undefined) {
            this.data.subject = query.subject;
        } else if (this.data.campaign && this.data.campaign.subject !== undefined) {
            this.data.subject = this.data.campaign.subject;
        } else {
            enforce(false);
        }

        this.data.configItems = await settings.get(contextHelpers.getAdminContext(), ['pgpPrivateKey', 'pgpPassphrase']);

        // log.verbose('DataCollector', `Collected settings data: ${JSON.stringify(this.data.configItems, null, ' ')}`);
    }

    collectAdditionalQueuedData(query) {
        this.data.type = query.type;
        this.data.status = CampaignMessageStatus.SCHEDULED;
        if (this.isQueuedCampaignMessage(query.type)) {
            this.data.list = query.list;
            this.data.subscription = query.subscription;
        } else {
            this.data.to = query.to;
        }
        this.data.hash_email = query.hash_email;
        this.data.hashEmailPiece = query.hashEmailPiece;
        this.data.mergeTags = query.mergeTags;
        this.data.encryptionKeys = query.encryptionKeys;
    }

    isCampaignMessage(messageType) {
        return messageType === MessageType.REGULAR;
    }

    isQueuedMessage(messageType) {
        return (messageType === MessageType.TRIGGERED || messageType === MessageType.TEST ||
            messageType === MessageType.SUBSCRIPTION || messageType === MessageType.API_TRANSACTIONAL);
    }

    isQueuedCampaignMessage(messageType) {
        return (messageType === MessageType.TRIGGERED || messageType === MessageType.TEST);
    }

    isQueuedNotCampaignMessage(messageType) {
        return (messageType === MessageType.SUBSCRIPTION || messageType === MessageType.API_TRANSACTIONAL);
    }
}

module.exports = DataCollector;
