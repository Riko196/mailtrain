'use strict';

const log = require('../lib/log');
const config = require('../lib/config');
const crypto = require('crypto');
const humanize = require('humanize');
const http = require('http');
const bluebird = require('bluebird');

const SMTPServer = require('smtp-server').SMTPServer;
const simpleParser = require('mailparser').simpleParser;

let totalMessages = 0;
let received = 0;

let sendingTime = 0;
let counter = 0;

const mailstore = {
    accounts: {},
    saveMessage(address, message) {
        console.log("SAVE MESSAGE START");
        if (!this.accounts[address]) {
            this.accounts[address] = [];
        }
        this.accounts[address].push(message);
        console.log("SAVE MESSAGE END");
    },
    getMail(address, callback) {
        console.log("GET MAIL START");
        if (!this.accounts[address] || this.accounts[address].length === 0) {
            const err = new Error('No mail for ' + address);
            err.status = 404;
            return callback(err);
        }
        simpleParser(this.accounts[address].shift(), (err, mail) => {
            console.log("SIMPLE PARSER START");
            if (err) {
                return callback(err.message || err);
            }
            callback(null, mail);
            console.log("SIMPLE PARSER END");
        });
        console.log("GET MAIL END");
    }
};

// Setup server
const server = new SMTPServer({

    // log to console
    logger: config.testServer.logger,

    // not required but nice-to-have
    banner: 'Welcome to My Awesome SMTP Server',

    // disable STARTTLS to allow authentication in clear text mode
    disabledCommands: ['STARTTLS'],

    // By default only PLAIN and LOGIN are enabled
    authMethods: ['PLAIN', 'LOGIN'],

    // Accept messages up to 10 MB
    size: 10 * 1024 * 1024,

    // Setup authentication
    onAuth: (auth, session, callback) => {
        // console.log("AUTH START");
        const username = config.testServer.username;
        const password = config.testServer.password;

        // check username and password
        if (auth.username === username && auth.password === password) {
            const result = callback(null, {
                user: 'userdata' // value could be an user id, or an user object etc. This value can be accessed from session.user afterwards
            });
            // console.log("AUTH END");
            return result;
        }

        // console.log("AUTH END FAILED");
        return callback(new Error('Authentication failed'));
    },

    // Validate MAIL FROM envelope address. Example allows all addresses that do not start with 'deny'
    // If this method is not set, all addresses are allowed
    onMailFrom: (address, session, callback) => {
        // console.log("ONMAIL START");
        counter++;
        const start = new Date().getTime();
        if (/^deny/i.test(address.address)) {
            return callback(new Error('Not accepted'));
        }
        callback();
        const end = new Date().getTime();
        sendingTime += end - start;
        if (counter >= 44846)
            console.log('testServer took ' + sendingTime / 1000);
         // console.log("ONMAIL END");
    },

    // Validate RCPT TO envelope address. Example allows all addresses that do not start with 'deny'
    // If this method is not set, all addresses are allowed
    onRcptTo: (address, session, callback) => {
        // console.log("ONRCP START");
        const start = new Date().getTime();
        if (/^deny/i.test(address.address)) {
            return callback(new Error('Not accepted'));
        }

        // Reject messages larger than 100 bytes to an over-quota user
        if (/^full/i.test(address.address) && Number(session.envelope.mailFrom.args.SIZE) > 100) {
            const err = new Error('Insufficient channel storage: ' + address.address);
            err.responseCode = 452;
            return callback(err);
        }

        callback();
        const end = new Date().getTime();
        sendingTime += end - start;
        if (counter >= 44846)
            console.log('testServer took ' + sendingTime / 1000);
        // console.log("ONRCP END");
    },

    // Handle message stream
    onData: (stream, session, callback) => {
        // console.log("ONDATA START");
        const start = new Date().getTime();
        const hash = crypto.createHash('md5');
        let message = '';
        stream.on('data', chunk => {
            // console.log("ONDATA STREAM START");
            const startStream = new Date().getTime();
            hash.update(chunk);
            if (/^keep/i.test(session.envelope.rcptTo[0].address)) {
                message += chunk;
            }
            const endStream = new Date().getTime();
            sendingTime += endStream - startStream;
            // console.log("ONDATA STREAM END");
        });
        stream.on('end', () => {
            // console.log("ONEND STERAM START");
            const startStream = new Date().getTime();
            if (stream.sizeExceeded) {
                const err = new Error('Error: message exceeds fixed maximum message size 10 MB');
                err.responseCode = 552;
                return callback(err);
            }

            // Store message for e2e tests
            if (/^keep/i.test(session.envelope.rcptTo[0].address)) {
                mailstore.saveMessage(session.envelope.rcptTo[0].address, message);
            }

            received++;
            callback(null, 'Message queued as ' + hash.digest('hex')); // accept the message once the stream is ended
            const endStream = new Date().getTime();
            sendingTime += endStream - startStream;
            // console.log("ONEND STREAM END");
        });
        const end = new Date().getTime();
        sendingTime += end - start;
        if (counter >= 44846)
            console.log('testServer took ' + sendingTime / 1000);
        // console.log("ONDATA END");
    }
});

server.on('error', err => {
    log.error('Test SMTP', err.stack);
});

const mailBoxServer = http.createServer((req, res) => {
    const renderer = data => (
        '<!doctype html><html><head><title>' + data.title + '</title></head><body>' + data.body + '</body></html>'
    );

    const address = req.url.substring(1);
    mailstore.getMail(address, (err, mail) => {
        // console.log("GET MAIL2 START");
        if (err) {
            const html = renderer({
                title: 'error',
                body: err.message || err
            });
            res.writeHead(err.status || 500, { 'Content-Type': 'text/html' });
            return res.end(html);
        }

        let html = mail.html || renderer({
            title: 'error',
            body: 'This mail has no HTML part'
        });

        // https://nodemailer.com/extras/mailparser/#mail-object
        delete mail.html;
        delete mail.textAsHtml;
        delete mail.attachments;

        const script = '<script> var mailObject = ' + JSON.stringify(mail) + '; console.log(mailObject); </script>';
        html = html.replace(/<\/body\b/i, match => script + match);
        html = html.replace(/target="_blank"/g, 'target="_self"');

        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(html);
        // console.log("GET MAIL2 END");
    });
});

mailBoxServer.on('error', err => {
    log.error('Test SMTP Mailbox Server', err);
});

function start(callback) {
    if (config.testServer.enabled) {
        server.listen(config.testServer.port, config.testServer.host, () => {
            log.info('Test SMTP', 'Server listening on port %s', config.testServer.port);

            setInterval(() => {
                // console.log("SET INTERVAL START");
                if (received) {
                    totalMessages += received;
                    log.verbose(
                        'Test SMTP',
                        'Received %s new message%s in last 60 sec. (total %s messages)',
                        humanize.numberFormat(received, 0), received === 1 ? '' : 's',
                        humanize.numberFormat(totalMessages, 0)
                    );
                    received = 0;
                }
                // console.log("SET INTERVAL END");
            }, 60 * 1000);

            mailBoxServer.listen(config.testServer.mailboxServerPort, config.testServer.host, () => {
                log.info('Test SMTP', 'Mail Box Server listening on port %s', config.testServer.mailboxServerPort);
                setImmediate(callback);
            });
        });
    } else {
        setImmediate(callback);
    }
}

module.exports.start = bluebird.promisify(start);
