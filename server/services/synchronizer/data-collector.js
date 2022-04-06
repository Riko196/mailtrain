
/**
 * DataCollector collects all needed data for processing one specific campaign from MySQL centralized database. Used by Synchronizer.
 */
class DataCollector {
    async collectData(query) {
        this = {};
        this.type = query.type;
        this.listsById = new Map(); // listId -> list
        this.listsByCid = new Map(); // listCid -> list
        this.listsFieldsGrouped = new Map(); // listId -> fieldsGrouped

        await knex.transaction(async tx => {
            if (this.type === MessageType.REGULAR || this.type === MessageType.TRIGGERED || this.type === MessageType.TEST) {
                this.isMassMail = true;

                await this.collectCampaign(tx, query);
                await this.collectSendConfiguration(tx, query);
                await this.collectLists(tx, query);
            } else if (this.type === MessageType.SUBSCRIPTION || this.type === MessageType.API_TRANSACTIONAL) {
                this.isMassMail = false;

                await this.collectRSSSendConfiguration(tx, query);
            } else {
                enforce(false);
            }

            await this.collectAttachments(tx, query);
            await this.collectTemplates(tx, query);
        });

        await this.collectSubscribers();
        await this.collectSettings(query);
    }

    async collectCampaign(tx, query) {
        if (query.campaign) {
            this.campaign = query.campaign;
        } else if (query.campaignCid) {
            this.campaign = await campaigns.rawGetByTx(tx, 'cid', query.campaignCid);
        } else if (query.campaignId) {
            this.campaign = await campaigns.rawGetByTx(tx, 'id', query.campaignId);
        }
    }

    async collectSendConfiguration(tx, query) {
        if (query.sendConfigurationId) {
            this.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(), query.sendConfigurationId, false, true);
        } else if (this.campaign && this.campaign.send_configuration) {
            this.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(), this.campaign.send_configuration, false, true);
        } else {
            enforce(false);
        }

        this.useVerp = config.verp.enabled && this.sendConfiguration.verp_hostname;
        this.useVerpSenderHeader = this.useVerp && !this.sendConfiguration.verp_disable_sender_header;
    }

    async collectRSSSendConfiguration(tx, query) {
        this.sendConfiguration = await sendConfigurations.getByIdTx(tx, contextHelpers.getAdminContext(), query.sendConfigurationId, false, true);
    }

    async collectLists(tx, query) {
        // These IFs are not mutually exclusive because there are situations when listId is provided, but we want to collect all lists of the campaign
        // in order to support tags like LINK_PUBLIC_SUBSCRIBE, LIST_ID_<index>, PUBLIC_LIST_ID_<index>
        if (query.listId) {
            const list = await lists.getByIdTx(tx, contextHelpers.getAdminContext(), query.listId);
            this.listsById.set(list.id, list);
            this.listsByCid.set(list.cid, list);
            this.listsFieldsGrouped.set(list.id, await fields.listGroupedTx(tx, list.id));
        }

        if (query.listCid && !this.listsByCid.has(query.listCid)) {
            const list = await lists.getByCidTx(tx, contextHelpers.getAdminContext(), query.listCid);
            this.listsById.set(list.id, list);
            this.listsByCid.set(list.cid, list);
            this.listsFieldsGrouped.set(list.id, await fields.listGroupedTx(tx, list.id));
        }

        if (this.campaign && this.campaign.lists) {
            for (const listSpec of this.campaign.lists) {
                if (!this.listsById.has(listSpec.list)) {
                    const list = await lists.getByIdTx(tx, contextHelpers.getAdminContext(), listSpec.list);
                    this.listsById.set(list.id, list);
                    this.listsByCid.set(list.cid, list);
                    this.listsFieldsGrouped.set(list.id, await fields.listGroupedTx(tx, list.id));
                }
            }
        }
    }

    async collectAttachments(tx, query) {
        if (query.attachments) {
            this.attachments = query.attachments;
        } else if (this.campaign && this.campaign.id) {
            const attachments = await files.listTx(tx, contextHelpers.getAdminContext(), 'campaign', 'attachment', this.campaign.id);

            this.attachments = [];
            for (const attachment of attachments) {
                this.attachments.push({
                    filename: attachment.originalname,
                    path: files.getFilePath('campaign', 'attachment', this.campaign.id, attachment.filename)
                });
            }
        } else {
            this.attachments = [];
        }
    }

    async collectTemplates(tx, query) {
        if (query.renderedHtml !== undefined) {
            this.renderedHtml = query.renderedHtml;
            this.renderedText = query.renderedText;
        } else if (query.html !== undefined) {
            this.html = query.html;
            this.text = query.text;
            this.tagLanguage = query.tagLanguage;
        } else if (this.campaign && this.campaign.source === CampaignSource.TEMPLATE) {
            this.template = await templates.getByIdTx(tx, contextHelpers.getAdminContext(), this.campaign.data.sourceTemplate, false);
            this.html = this.template.html;
            this.text = this.template.text;
            this.tagLanguage = this.template.tag_language;
        } else if (this.campaign &&
            (this.campaign.source === CampaignSource.CUSTOM || this.campaign.source === CampaignSource.CUSTOM_FROM_TEMPLATE || this.campaign.source === CampaignSource.CUSTOM_FROM_CAMPAIGN)) {
            this.html = this.campaign.data.sourceCustom.html;
            this.text = this.campaign.data.sourceCustom.text;
            this.tagLanguage = this.campaign.data.sourceCustom.tag_language;
        }

        enforce(this.renderedHtml || (this.campaign && this.campaign.source === CampaignSource.URL) || this.tagLanguage);
    }

    async collectSubscribers() {
        this.subscribers = {};
        for (const listId of Object.keys(this.listsById)) {
            this.subscribers[getSubscriptionTableName(listId)] = await subscriptions.list(contextHelpers.getAdminContext(), listId, false, null, null);
        }
    }

    async collectSettings(query) {
        if (query.rssEntry !== undefined) {
            this.rssEntry = query.rssEntry;
        } else if (this.campaign && this.campaign.data.rssEntry) {
            this.rssEntry = this.campaign.data.rssEntry;
        }

        if (query.subject !== undefined) {
            this.subject = query.subject;
        } else if (this.campaign && this.campaign.subject !== undefined) {
            this.subject = this.campaign.subject;
        } else {
            enforce(false);
        }

        this.configItems = await settings.get(contextHelpers.getAdminContext(), ['pgpPrivateKey', 'pgpPassphrase']);
    }
}
