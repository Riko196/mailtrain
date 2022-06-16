'use strict';

const config = require('../../config');
const log = require('../../log');
const { CampaignMessageErrorType } = require('../../../../shared/campaigns');
const openpgpEncrypt = require('nodemailer-openpgp').openpgpEncrypt;
const { getMongoDB } = require('../../mongodb');
const { ZoneMTAType, MailerType } = require('../../../../shared/send-configurations');
const builtinZoneMta = require('../../builtin-zone-mta');
const nodemailer = require('nodemailer');
const aws = require('aws-sdk');
const bluebird = require('bluebird');

/**
 * The main (abstract) class which takes a made mail for some specific subsriber and sends it to SMTP server.
 */
class MailSender {
    constructor(campaignData, blacklisted) {
        Object.assign(this, campaignData);
        this.mongodb = getMongoDB();
        /* email -> blacklisted */
        this.blacklisted = blacklisted;
        /* sendConfigurationID -> transport */
        this.transports = new Map();
    }

    async getOrCreateMailer(sendConfiguration) {
        const transport = this.transports.get(sendConfiguration.id)
            || await this.createTransport(sendConfiguration);
        return transport.mailer;
    }

    invalidateMailer(sendConfigurationId) {
        this.transports.delete(sendConfigurationId);
    }

    addDkimKeys(transport, mail) {
        const sendConfiguration = transport.mailer.sendConfiguration;

        if (sendConfiguration.mailer_type === MailerType.ZONE_MTA) {
            const mailerSettings = sendConfiguration.mailer_settings;

            if (mailerSettings.zoneMtaType === ZoneMTAType.WITH_MAILTRAIN_HEADER_CONF
                || mailerSettings.zoneMtaType === ZoneMTAType.BUILTIN) {
                if (!mail.headers) {
                    mail.headers = {};
                }

                const dkimDomain = mailerSettings.dkimDomain;
                const dkimSelector = (mailerSettings.dkimSelector || '').trim();
                const dkimPrivateKey = (mailerSettings.dkimPrivateKey || '').trim();

                if (dkimSelector && dkimPrivateKey) {
                    const from = (mail.from.address || '').trim();
                    const domain = from.split('@').pop().toLowerCase().trim();

                    mail.headers['x-mailtrain-dkim'] = JSON.stringify({
                        domainName: dkimDomain || domain,
                        keySelector: dkimSelector,
                        privateKey: dkimPrivateKey
                    });
                }
            }
        }
    }

    async sendMailToSMTP(transport, mail, template) {
        this.addDkimKeys(transport, mail);

        try {
            return await transport.sendMailAsync(mail);
        } catch (err) {
            if ((err.responseCode && err.responseCode >= 400 && err.responseCode < 500)
                || (err.code === 'ECONNECTION' && err.errno === 'ECONNREFUSED')) {
                throw new SendConfigurationError(transport.mailer.sendConfiguration.id, 'Cannot connect to service specified by send configuration ' + transport.mailer.sendConfiguration.id);
            }

            throw err;
        }
    }

    async sendTransactionalMailToSMTP(transport, mail) {
        if (!mail.headers) {
            mail.headers = {};
        }
        mail.headers['X-Sending-Zone'] = 'transactional';

        return await this.sendMailToSMTP(transport, mail);
    }

    async createTransport(sendConfiguration) {
        const mailerSettings = sendConfiguration.mailer_settings;
        const mailerType = sendConfiguration.mailer_type;
        const configItems = this.configItems;

        const existingTransport = this.transports.get(sendConfiguration.id);

        let existingListeners = [];
        if (existingTransport) {
            existingListeners = existingTransport.listeners('idle');
            existingTransport.removeAllListeners('idle');
            existingTransport.removeAllListeners('stream');
            existingTransport.throttleWait = null;
        }

        const logFunc = (...args) => {
            const level = args.shift();
            args.shift();
            args.unshift('Mail');
            log[level](...args);
        };


        let transportOptions;

        if (mailerType === MailerType.GENERIC_SMTP || mailerType === MailerType.ZONE_MTA) {
            transportOptions = {
                pool: true,
                debug: mailerSettings.logTransactions,
                logger: mailerSettings.logTransactions ? {
                    debug: logFunc.bind(null, 'verbose'),
                    info: logFunc.bind(null, 'info'),
                    error: logFunc.bind(null, 'error')
                } : false,
                maxConnections: mailerSettings.maxConnections,
                maxMessages: mailerSettings.maxMessages,
                tls: {
                    rejectUnauthorized: !mailerSettings.allowSelfSigned
                }
            };

            if (mailerType === MailerType.ZONE_MTA && mailerSettings.zoneMtaType === ZoneMTAType.BUILTIN) {
                transportOptions.host = config.builtinZoneMTA.host;
                transportOptions.port = config.builtinZoneMTA.port;
                transportOptions.secure = false;
                transportOptions.ignoreTLS = true;
                transportOptions.auth = {
                    user: builtinZoneMta.getUsername(),
                    pass: builtinZoneMta.getPassword()
                };
            } else {
                transportOptions.host = mailerSettings.hostname;
                transportOptions.port = mailerSettings.port || false;
                transportOptions.secure = mailerSettings.encryption === 'TLS';
                transportOptions.ignoreTLS = mailerSettings.encryption === 'NONE';
                transportOptions.auth = mailerSettings.useAuth ? {
                    user: mailerSettings.user,
                    pass: mailerSettings.password
                } : false;
            }

        } else if (mailerType === MailerType.AWS_SES) {
            const sendingRate = mailerSettings.throttling / 3600;  // convert to messages/second

            transportOptions = {
                SES: new aws.SES({
                    apiVersion: '2010-12-01',
                    accessKeyId: mailerSettings.key,
                    secretAccessKey: mailerSettings.secret,
                    region: mailerSettings.region
                }),
                debug: mailerSettings.logTransactions,
                logger: mailerSettings.logTransactions ? {
                    debug: logFunc.bind(null, 'verbose'),
                    info: logFunc.bind(null, 'info'),
                    error: logFunc.bind(null, 'error')
                } : false,
                maxConnections: mailerSettings.maxConnections,
                sendingRate
            };

        } else {
            throw new Error('Invalid mail transport');
        }

        if (config.nodemailer.testReal) {
            transportOptions = {
                service: config.nodemailer.service,
                auth: {
                  user: config.nodemailer.user,
                  pass: config.nodemailer.password
                }
            };
        }

        const transport = nodemailer.createTransport(transportOptions, config.nodemailer);
        transport.sendMailAsync = bluebird.promisify(transport.sendMail.bind(transport));

        transport.use('stream', openpgpEncrypt({
            signingKey: configItems.pgpPrivateKey,
            passphrase: configItems.pgpPassphrase
        }));

        if (existingListeners.length) {
            log.info('Mail', 'Reattaching %s idle listeners', existingListeners.length);
            existingListeners.forEach(listener => transport.on('idle', listener));
        }

        let throttleWait;

        if (mailerType === MailerType.GENERIC_SMTP || mailerType === MailerType.ZONE_MTA) {
            let throttling = mailerSettings.throttling;
            if (throttling) {
                throttling = 1 / (throttling / (3600 * 1000));
            }

            let lastCheck = Date.now();

            throttleWait = function (next) {
                if (!throttling) {
                    return next();
                }
                let nextCheck = Date.now();
                let checkDiff = (nextCheck - lastCheck);
                if (checkDiff < throttling) {
                    log.verbose('Mail', 'Throttling next message in %s sec.', (throttling - checkDiff) / 1000);
                    setTimeout(() => {
                        lastCheck = Date.now();
                        next();
                    }, throttling - checkDiff);
                } else {
                    lastCheck = nextCheck;
                    next();
                }
            };
        } else {
            throttleWait = next => next();
        }

        transport.mailer = {
            sendConfiguration,
            throttleWait: bluebird.promisify(throttleWait),
            sendTransactionalMail: async (mail) => await this.sendTransactionalMailToSMTP(transport, mail),
            sendMassMail: async (mail, template) => await this.sendMailToSMTP(transport, mail)
        };

        this.transports.set(sendConfiguration.id, transport);
        return transport;
    }

    async sendMail(mail) {
        //log.verbose('MailSender', `Starting to sending mail for ${mail.to.address} ...`);
        if (this.blacklisted.get(mail.to)) {
            return {};
        }

        const transport = await this.getOrCreateMailer(this.sendConfiguration);
        await transport.throttleWait();

        try {
            const info = this.isMassMail ?
                await transport.sendMassMail(mail) :
                await transport.sendTransactionalMail(mail);

            //log.verbose('MailSender', `response: ${info.response} messageId: ${info.messageId}`);

            return this.analyzeResponse(info);
        } catch (error) {
            if (error.responseCode >= 500) {
                error.campaignMessageErrorType = CampaignMessageErrorType.PERMANENT;
            } else {
                error.campaignMessageErrorType = CampaignMessageErrorType.TRANSIENT;
            }
            throw error;
        }
    }

    analyzeResponse(info) {
        let response, responseId, match;
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

        return { response, responseId };
    }
}

class SendConfigurationError extends Error {
    constructor(sendConfigurationId, ...args) {
        super(...args);
        this.sendConfigurationId = sendConfigurationId;
        Error.captureStackTrace(this, SendConfigurationError);
    }
}

class NodemailerError extends Error {
    constructor(mail, responseCode) {
        super(mail);
        this.responseCode = responseCode;
    }
}

module.exports.MailSender = MailSender;
module.exports.SendConfigurationError = SendConfigurationError;
module.exports.NodemailerError = NodemailerError;
