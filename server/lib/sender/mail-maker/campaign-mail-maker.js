'use strict';

const MailMaker = require('./mail-maker');
const subscriptions = require('../../../models/subscriptions');
const fields = require('../../../models/fields');
const { getPublicUrl } = require('../../urls');
const libmime = require('libmime');
const { toNameTagLangauge, getFieldColumn } = require('../../../../shared/lists');
const tools = require('../../tools');

/**
 * The class which inherits from MailSender and is responsible for making mails of campaign (Regular, RSS, Triggered, Test) messages.
 */
class CampaignMailMaker extends MailMaker {
    constructor(campaignData, subscribers) {
        super(campaignData);
        /* listID:subscription -> subscriber */
        this.subscribers = subscribers;
        this.listsById = JSON.parse(this.listsById);
        this.listsByCid = JSON.parse(this.listsByCid);
        this.listsFieldsGrouped = JSON.parse(this.listsFieldsGrouped);
    }

    /*
     *  CampaignMessage:
     *      - listId
     *      - subscriptionId
     *      - mergeTags [optional, used only when campaign / html+text is provided]
     */
    async makeMail(campaignMessage) {
        const mail = { envelope: false, sender: false, headers: {}, listHeader: false, encryptionKeys: [] };
        const listId = campaignMessage.list;
        const subscriptionId = campaignMessage.subscription;

        let subscriptionGrouped = {};
        if (subscriptionId) {
            const subscriber = this.subscribers.get(`${listId}:${subscriptionId}`);
            const groupedFieldsMap = {};
            for (const field of this.listsFieldsGrouped[listId]) {
                groupedFieldsMap[getFieldColumn(field)] = field;
            }

            subscriptions.groupSubscription(groupedFieldsMap, subscriber);
            subscriptionGrouped = subscriber;
        }

        const list = this.listsById[listId];
        const mailFields = this.listsFieldsGrouped[list.id];

        let mergeTags = campaignMessage.mergeTags;
        if (!mergeTags) {
            mergeTags = fields.getMergeTags(mailFields, subscriptionGrouped, this.getExtraTags());
        }

        for (const field of mailFields) {
            if (field.type === 'gpg' && mergeTags[field.key]) {
                mail.encryptionKeys.push(mergeTags[field.key].trim());
            }
        }

        const message = await this.makeMessage(mergeTags, list, subscriptionGrouped, true);

        let listUnsubscribe = null;
        if (!list.listunsubscribe_disabled) {
            listUnsubscribe = this.campaign && this.campaign.unsubscribe_url
                ? tools.formatCampaignTemplate(this.campaign.unsubscribe_url, this.tagLanguage, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped)
                : getPublicUrl('/subscription/' + list.cid + '/unsubscribe/' + subscriptionGrouped.cid);
        }

        mail.to = {
            name: list.to_name === null ?
                undefined :
                tools.formatCampaignTemplate(list.to_name, toNameTagLangauge, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped),
            address: subscriptionGrouped.email
        };

        mail.subject = this.subject;

        if (this.tagLanguage) {
            mail.subject = tools.formatCampaignTemplate(this.subject, this.tagLanguage, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped);
        }

        mail.headers = {
            'List-ID': {
                prepared: true,
                value: libmime.encodeWords(list.name) + ' <' + list.cid + '.' + getPublicUrl() + '>'
            }
        };

        if (this.campaign) {
            const campaignAddress = [this.campaign.cid, list.cid, subscriptionGrouped.cid].join('.');

            if (this.useVerp) {
                mail.envelope = {
                    from: campaignAddress + '@' + this.sendConfiguration.verp_hostname,
                    to: subscriptionGrouped.email
                };
            }

            if (this.useVerpSenderHeader) {
                mail.sender = campaignAddress + '@' + this.sendConfiguration.verp_hostname;
            }

            mail.headers['x-fbl'] = campaignAddress;
            mail.headers['x-msys-api'] = JSON.stringify({
                campaign_id: campaignAddress
            });
            mail.headers['x-smtpapi'] = JSON.stringify({
                unique_args: {
                    campaign_id: campaignAddress
                }
            });
            mail.headers['x-mailgun-variables'] = JSON.stringify({
                campaign_id: campaignAddress
            });
        }

        mail.listHeader = {
            unsubscribe: listUnsubscribe
        };

        return this.accomplishMail(mail, message);
    }

    getExtraTags() {
        const tags = {};

        if (this.rssEntry) {
            const rssEntry = this.rssEntry;
            tags.RSS_ENTRY_TITLE = rssEntry.title;
            tags.RSS_ENTRY_DATE = rssEntry.date;
            tags.RSS_ENTRY_LINK = rssEntry.link;
            tags.RSS_ENTRY_CONTENT = rssEntry.content;
            tags.RSS_ENTRY_SUMMARY = rssEntry.summary;
            tags.RSS_ENTRY_IMAGE_URL = rssEntry.imageUrl;
            tags.RSS_ENTRY_CUSTOM_TAGS = rssEntry.customTags;
        }

        return tags;
    }
}

module.exports = CampaignMailMaker;
