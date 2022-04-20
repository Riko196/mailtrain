'use strict';

const config = require('../config');
const log = require('../log');
const MailSender = require('./mailer');
const contextHelpers = require('../context-helpers');
const { CampaignSource, CampaignType, CampaignMessageStatus, CampaignMessageErrorType } = require('../../../shared/campaigns');
const { toNameTagLangauge } = require('../../../shared/lists');
const { MessageType } = require('../../../shared/messages');
const tools = require('../tools');
const htmlToText = require('html-to-text');
const request = require('request-promise');
const { getPublicUrl } = require('../urls');
const libmime = require('libmime');
const { enforce, hashEmail } = require('../helpers');
const senders = require('../senders');
const shortid = require('../shortid');

class MessageSender {
    constructor() {
    }

    async _getMessage(mergeTags, list, subscriptionGrouped, replaceDataImgs) {
        let html = '';
        let text = '';
        let renderTags = false;
        const campaign = this.campaign;


        if (this.renderedHtml !== undefined) {
            html = this.renderedHtml;
            text = this.renderedText;
            renderTags = false;

        } else if (this.html !== undefined) {
            enforce(mergeTags);

            html = this.html;
            text = this.text;
            renderTags = true;

        } else if (campaign && campaign.source === CampaignSource.URL) {
            const form = tools.getMessageLinks(campaign, this.listsById, list, subscriptionGrouped);
            for (const key in mergeTags) {
                form[key] = mergeTags[key];
            }

            const sourceUrl = campaign.data.sourceUrl;

            let response;
            try {
                response = await request.post({
                    uri: sourceUrl,
                    form,
                    resolveWithFullResponse: true
                });
            } catch (exc) {
                log.error('MessageSender', `Error pulling content from URL (${sourceUrl})`);
                response = {statusCode: exc.message};
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

            html = response.body;
            text = '';
            renderTags = false;
        }

        const attachments = this.attachments.slice();
        if (replaceDataImgs) {
            // replace data: images with embedded attachments
            html = html.replace(/(<img\b[^>]* src\s*=[\s"']*)(data:[^"'>\s]+)/gi, (match, prefix, dataUri) => {
                const cid = shortid.generate() + '-attachments';
                attachments.push({
                    path: dataUri,
                    cid
                });

                return prefix + 'cid:' + cid;
            });
        }


        if (renderTags) {
            if (campaign) {
                html = await links.updateLinks(html, this.tagLanguage, mergeTags, campaign, this.listsById, list, subscriptionGrouped);
                console.log('LINK: ' + JSON.stringify(html, null, 4));
            }

            // When no list and subscriptionGrouped is provided, formatCampaignTemplate works the same way as formatTemplate
            html = tools.formatCampaignTemplate(html, this.tagLanguage, mergeTags, true, campaign, this.listsById, list, subscriptionGrouped);
        }

        const generateText = !(text || '').trim();
        if (generateText) {
            text = htmlToText.fromString(html, {wordwrap: 130});
        } else {
            // When no list and subscriptionGrouped is provided, formatCampaignTemplate works the same way as formatTemplate
            text = tools.formatCampaignTemplate(text, this.tagLanguage, mergeTags, false, campaign, this.listsById, list, subscriptionGrouped);
        }

        return {
            html,
            text,
            attachments
        };
    }

    _getExtraTags() {
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
    async _sendMessage(subData) {
        let to, email;
        let envelope = false;
        let sender = false;
        let headers = {};
        let listHeader = false;
        let encryptionKeys = [];
        let subject;
        let message;

        let subscriptionGrouped, list; // May be undefined
        const campaign = this.campaign; // May be undefined

        const sendConfiguration = this.sendConfiguration;

        let mergeTags = subData.mergeTags;

        if (subData.listId) {
            let listId;

            const startGetById = new Date().getTime();
            if (subData.subscriptionId) {
                listId = subData.listId;
                subscriptionGrouped = await subscriptions.getById(contextHelpers.getAdminContext(), listId, subData.subscriptionId);
                console.log('SUBSCRIBER: ' + JSON.stringify(subscriptionGrouped, null, 4));
            }

            list = this.listsById.get(listId);
            email = subscriptionGrouped.email;

            const flds = this.listsFieldsGrouped.get(list.id);

            if (!mergeTags) {
                mergeTags = fields.getMergeTags(flds, subscriptionGrouped, this._getExtraTags());
            }

            for (const fld of flds) {
                if (fld.type === 'gpg' && mergeTags[fld.key]) {
                    encryptionKeys.push(mergeTags[fld.key].trim());
                }
            }

            message = await this._getMessage(mergeTags, list, subscriptionGrouped, true);

            let listUnsubscribe = null;
            if (!list.listunsubscribe_disabled) {
                listUnsubscribe = campaign && campaign.unsubscribe_url
                    ? tools.formatCampaignTemplate(campaign.unsubscribe_url, this.tagLanguage, mergeTags, false, campaign, this.listsById, list, subscriptionGrouped)
                    : getPublicUrl('/subscription/' + list.cid + '/unsubscribe/' + subscriptionGrouped.cid);
            }

            to = {
                name: list.to_name === null ? undefined : tools.formatCampaignTemplate(list.to_name, toNameTagLangauge, mergeTags, false, campaign, this.listsById, list, subscriptionGrouped),
                address: subscriptionGrouped.email
            };

            subject = this.subject;

            if (this.tagLanguage) {
                subject = tools.formatCampaignTemplate(this.subject, this.tagLanguage, mergeTags, false, campaign, this.listsById, list, subscriptionGrouped);
            }

            headers = {
                'List-ID': {
                    prepared: true,
                    value: libmime.encodeWords(list.name) + ' <' + list.cid + '.' + getPublicUrl() + '>'
                }
            };

            if (campaign) {
                const campaignAddress = [campaign.cid, list.cid, subscriptionGrouped.cid].join('.');

                if (this.useVerp) {
                    envelope = {
                        from: campaignAddress + '@' + sendConfiguration.verp_hostname,
                        to: subscriptionGrouped.email
                    };
                }

                if (this.useVerpSenderHeader) {
                    sender = campaignAddress + '@' + sendConfiguration.verp_hostname;
                }

                headers['x-fbl'] = campaignAddress;
                headers['x-msys-api'] = JSON.stringify({
                    campaign_id: campaignAddress
                });
                headers['x-smtpapi'] = JSON.stringify({
                    unique_args: {
                        campaign_id: campaignAddress
                    }
                });
                headers['x-mailgun-variables'] = JSON.stringify({
                    campaign_id: campaignAddress
                });
            }

            listHeader = {
                unsubscribe: listUnsubscribe
            };

        } else if (subData.to) {
            to = subData.to;
            email = to.address;
            subject = this.subject;
            encryptionKeys = subData.encryptionKeys;
            message = await this._getMessage(mergeTags);
        }

        if (await blacklist.isBlacklisted(email)) {
            return;
        }


        const mailer = await mailers.getOrCreateMailer(sendConfiguration.id);

        await mailer.throttleWait();
        const getOverridable = key => {
            if (campaign && sendConfiguration[key + '_overridable'] && campaign[key + '_override'] !== null) {
                return campaign[key + '_override'] || '';
            } else {
                return sendConfiguration[key] || '';
            }
        };

        const mail = {
            from: {
                name: getOverridable('from_name'),
                address: getOverridable('from_email')
            },
            replyTo: getOverridable('reply_to'),
            xMailer: sendConfiguration.x_mailer ? sendConfiguration.x_mailer : false,
            to,
            sender,
            envelope,
            headers,
            list: listHeader,
            subject,
            html: message.html,
            text: message.text,
            attachments: message.attachments,
            encryptionKeys
        };


        let response;
        let responseId = null;

        let info;
        try {
            info = this.isMassMail ? await mailer.sendMassMail(mail) : await mailer.sendTransactionalMail(mail);
        } catch (err) {
            if (err.responseCode >= 500) {
                err.campaignMessageErrorType = CampaignMessageErrorType.PERMANENT;
            } else {
                err.campaignMessageErrorType = CampaignMessageErrorType.TRANSIENT;
            }
            throw err;
        }

        log.verbose('MessageSender', `response: ${info.response}   messageId: ${info.messageId}`);

        let match;
        if ((match = info.response.match(/^250 Message queued as ([0-9a-f]+)$/))) {
            /*
                ZoneMTA
                info.response: 250 Message queued as 1691ad7f7ae00080fd
                info.messageId: <e65c9386-e899-7d01-b21e-ec03c3a9d9b4@sathyasai.org>
             */
            response = info.response;
            responseId = match[1];

        } else if ((match = info.messageId.match(/^<([^>@]*)@.*amazonses\.com>$/))) {
            /*
                AWS SES
                info.response: 0102016ad2244c0a-955492f2-9194-4cd1-bef9-70a45906a5a7-000000
                info.messageId: <0102016ad2244c0a-955492f2-9194-4cd1-bef9-70a45906a5a7-000000@eu-west-1.amazonses.com>
             */
            response = info.response;
            responseId = match[1];

        } else if (info.response.match(/^250 OK$/) && (match = info.messageId.match(/^<([^>]*)>$/))) {
            /*
                Postal Mail Server
                info.response: 250 OK
                info.messageId:  <xxxxxxxxx@xxx.xx> (postal messageId)
             */
            response = info.response;
            responseId = match[1];

        } else {
            /*
                Fallback - Mailtrain v1 behavior
             */
            response = info.response || info.messageId;
            responseId = response.split(/\s+/).pop();
        }


        const result = {
            response,
            responseId,
            list,
            subscriptionGrouped,
            email
        };

        return result;
    }

    async sendRegularCampaignMessage(campaignMessage) {
        enforce(this.type === MessageType.REGULAR);

        // We set the campaign_message to SENT before the message is actually sent. This is to avoid multiple delivery
        // if by chance we run out of disk space and couldn't change status in the database after the message has been sent out
        await knex('campaign_messages')
            .where({id: campaignMessage.id})
            .update({
                status: CampaignMessageStatus.SENT,
                updated: new Date()
            });

        let result;
        try {
            result = await this._sendMessage({listId: campaignMessage.list, subscriptionId: campaignMessage.subscription});
        } catch (err) {
            if (err.campaignMessageErrorType === CampaignMessageErrorType.PERMANENT) {
                await knex('campaign_messages')
                    .where({id: campaignMessage.id})
                    .update({
                        status: CampaignMessageStatus.FAILED,
                        updated: new Date()
                    });
            } else {
                await knex('campaign_messages')
                    .where({id: campaignMessage.id})
                    .update({
                        status: CampaignMessageStatus.SCHEDULED,
                        updated: new Date()
                    });
            }
            throw err;
        }

        enforce(result.list);
        enforce(result.subscriptionGrouped);

        await knex('campaign_messages')
            .where({id: campaignMessage.id})
            .update({
                response: result.response,
                response_id: result.responseId,
                updated: new Date()
            });

        await knex('campaigns').where('id', this.campaign.id).increment('delivered');
    }
}

module.exports = MailMaker;
