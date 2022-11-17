'use strict';

const log = require('../../log');
const links = require('../../../models/links');
const { CampaignSource } = require('../../../../shared/campaigns');
const { MessageErrorType } = require('../../../../shared/messages');
const tools = require('../../tools');
const htmlToText = require('html-to-text');
const request = require('request-promise');
const { enforce } = require('../../helpers');
const shortid = require('../../shortid');

/**
 * The main (abstract) class which makes the whole mail for some specific subsriber.
 */
class MailMaker {
    constructor(taskData) {
        Object.assign(this, taskData);
        this.links = [];
    }

    /* Make html and text part of the mail. */
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
                    statusError.campaignMessageErrorType = MessageErrorType.PERMANENT;
                } else {
                    statusError.campaignMessageErrorType = MessageErrorType.TRANSIENT;
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

    /* Abstract method for making mail from message */
    async makeMail(data) {
    }

    getOverridable(key) {
        if (this.campaign && this.sendConfiguration[key + '_overridable'] && this.campaign[key + '_override'] !== null) {
            return this.campaign[key + '_override'] || '';
        } else {
            return this.sendConfiguration[key] || '';
        }
    }

    accomplishMail(mail, message) {
        Object.assign(mail, message);
        mail.from = {
            name: this.getOverridable('from_name'),
            address: this.getOverridable('from_email')
        };
        mail.replyTo = this.getOverridable('reply_to');
        mail.xMailer = this.sendConfiguration.x_mailer ? this.sendConfiguration.x_mailer : false;
        return mail;
    }
}

module.exports = MailMaker;
