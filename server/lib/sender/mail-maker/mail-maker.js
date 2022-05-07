'use strict';

const log = require('../../log');
const { groupSubscription, getSubscriptionTableName } = require('../../../models/subscriptions');
const fields = require('../../../models/fields');
const links = require('../../../models/links');
const { getMongoDB } = require('../../mongodb');
const { CampaignSource, CampaignMessageErrorType } = require('../../../../shared/campaigns');
const { toNameTagLangauge, getFieldColumn } = require('../../../../shared/lists');
const tools = require('../../tools');
const htmlToText = require('html-to-text');
const request = require('request-promise');
const { getPublicUrl } = require('../../urls');
const libmime = require('libmime');
const { enforce } = require('../../helpers');
const shortid = require('../../shortid');

/**
 * The main (abstract) class which makes the whole mail for some specific subsriber.
 */
class MailMaker {
    constructor(campaignData) {
        Object.assign(this, campaignData);
        this.mongodb = getMongoDB();
        this.listsById = JSON.parse(this.listsById);
        this.listsByCid = JSON.parse(this.listsByCid);
        this.listsFieldsGrouped = JSON.parse(this.listsFieldsGrouped);
    }

    async makeMessage(mergeTags, list, subscriptionGrouped, replaceDataImgs) {
        const message = { html: '', text: '', renderTags: false };

        if (this.renderedHtml !== undefined) {
            message.html = this.renderedHtml;
            message.text = this.renderedText;
            message.renderTags = false;
        } else if (this.html !== undefined) {
            enforce(mergeTags);
            message.html = this.html;
            message.text = this.text;
            message.renderTags = true;
        } else if (this.campaign && this.campaign.source === CampaignSource.URL) {
            const form = tools.getMessageLinks(this.campaign, this.listsById, list, subscriptionGrouped);
            for (const key in mergeTags) {
                form[key] = mergeTags[key];
            }

            const sourceUrl = this.campaign.data.sourceUrl;

            let response;
            try {
                response = await request.post({
                    uri: sourceUrl,
                    form,
                    resolveWithFullResponse: true
                });
            } catch (error) {
                log.error('MailMaker', `Error pulling content from URL (${sourceUrl})`);
                response = { statusCode: error.message };
            }

            if (response.statusCode !== 200) {
                const statusError = new Error(`Received status code ${response.statusCode} from ${sourceUrl}`);
                if (response.statusCode >= 500) {
                    statusError.campaignMessageErrorType = CampaignMessageErrorType.PERMANENT;
                } else {
                    statusError.campaignMessageErrorType = CampaignMessageErrorType.TRANSIENT;
                }
                throw statusError;
            }

            message.html = response.body;
            message.text = '';
            message.renderTags = false;
        }

        message.attachments = this.attachments.slice();
        if (replaceDataImgs) {
            // replace data: images with embedded attachments
            message.html = message.html.replace(/(<img\b[^>]* src\s*=[\s"']*)(data:[^"'>\s]+)/gi, (match, prefix, dataUri) => {
                const cid = shortid.generate() + '-attachments';
                message.attachments.push({
                    path: dataUri,
                    cid
                });

                return prefix + 'cid:' + cid;
            });
        }


        if (message.renderTags) {
            if (this.campaign) {
                message.html = links.updateLinks(message.html, this.tagLanguage, mergeTags, this.campaign, this.listsById, list, subscriptionGrouped, this.links);
            }

            // When no list and subscriptionGrouped is provided, formatCampaignTemplate works the same way as formatTemplate
            message.html = tools.formatCampaignTemplate(message.html, this.tagLanguage, mergeTags, true, this.campaign, this.listsById, list, subscriptionGrouped);
        }

        const generateText = !(message.text || '').trim();
        if (generateText) {
            message.text = htmlToText.fromString(message.html, {wordwrap: 130});
        } else {
            // When no list and subscriptionGrouped is provided, formatCampaignTemplate works the same way as formatTemplate
            message.text = tools.formatCampaignTemplate(message.text, this.tagLanguage, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped);
        }

        return message;
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

    /*
        Accepted combinations of subData:

        Option #1
        - listId
        - subscriptionId
        - mergeTags [optional, used only when campaign / html+text is provided]

        Option #2:
        - to ... email / { name, address }
        - encryptionKeys [optional]
        - mergeTags [used only when campaign / html+text is provided]
     */
    async makeMail(subData) {
        const mail = { envelope: false, sender: false, headers: {}, listHeader: false, encryptionKeys: [] };


        const sendConfiguration = this.sendConfiguration;

        let mergeTags = subData.mergeTags;
        let message;
        /* Regular campaign */
        if (subData.listId) {
            let listId;
            let subscriptionGrouped = {};
            if (subData.subscriptionId) {
                listId = subData.listId;
                const subscriber = await this.mongodb
                    .collection(getSubscriptionTableName(listId))
                    .findOne({ _id: subData.subscriptionId });
                const groupedFieldsMap = {};
                for (const field of this.listsFieldsGrouped[listId]) {
                    groupedFieldsMap[getFieldColumn(field)] = field;
                }

                groupSubscription(groupedFieldsMap, subscriber);
                subscriptionGrouped = subscriber;
            }

            const list = this.listsById[listId];
            const mailFields = this.listsFieldsGrouped[list.id];

            if (!mergeTags) {
                mergeTags = fields.getMergeTags(mailFields, subscriptionGrouped, this.getExtraTags());
            }

            for (const field of mailFields) {
                if (field.type === 'gpg' && mergeTags[field.key]) {
                    mail.encryptionKeys.push(mergeTags[field.key].trim());
                }
            }

            message = await this.makeMessage(mergeTags, list, subscriptionGrouped, true);

            let listUnsubscribe = null;
            if (!list.listunsubscribe_disabled) {
                listUnsubscribe = this.campaign && this.campaign.unsubscribe_url
                    ? tools.formatCampaignTemplate(this.campaign.unsubscribe_url, this.tagLanguage, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped)
                    : getPublicUrl('/subscription/' + list.cid + '/unsubscribe/' + subscriptionGrouped.cid);
            }

            mail.to = {
                name: list.to_name === null ? undefined : tools.formatCampaignTemplate(list.to_name, toNameTagLangauge, mergeTags, false, this.campaign, this.listsById, list, subscriptionGrouped),
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

        } else if (subData.to) {
            mail.to = subData.to;
            mail.subject = this.subject;
            mail.encryptionKeys = subData.encryptionKeys;
            message = await this.makeMessage(mergeTags);
        }

        const getOverridable = key => {
            if (this.campaign && this.sendConfiguration[key + '_overridable'] && this.campaign[key + '_override'] !== null) {
                return this.campaign[key + '_override'] || '';
            } else {
                return this.sendConfiguration[key] || '';
            }
        };

        Object.assign(mail, message);
        mail.from = {
            name: getOverridable('from_name'),
            address: getOverridable('from_email')
        };
        mail.replyTo = getOverridable('reply_to');
        mail.xMailer = this.sendConfiguration.x_mailer ? this.sendConfiguration.x_mailer : false;
        return mail;
    }
}

module.exports = MailMaker;
