'use strict';

const path = require('path');
const bluebird = require('bluebird');
const fork = require('../fork').fork;
const knex = require('../knex');
const log = require('../log');
const builtinZoneMta = require('../builtin-zone-mta');
const { CampaignStatus } = require('../../../shared/campaigns');

let messageTid = 0;
let senderProcess;

function spawn(callback) {
    log.verbose('Sender', 'Spawning synchronizer process');

    /* Setup synchronizing camapaigns to scheduled status again */
    knex('campaigns').where('status', CampaignStatus.SYNCHRONIZING).update({ status: CampaignStatus.SCHEDULED })
        .then(() => {
            senderProcess = fork(path.join(__dirname, '..', '..', 'services', 'synchronizer.js'), [], {
                cwd: path.join(__dirname, '..', '..'),
                env: {
                    NODE_ENV: process.env.NODE_ENV,
                    BUILTIN_ZONE_MTA_PASSWORD: builtinZoneMta.getPassword()
                }
            });

            senderProcess.on('message', msg => {
                if (msg) {
                    if (msg.type === 'synchronizer-started') {
                        log.info('Sender', 'Synchronizer started');
                        return callback();
                    }
                }
            });

            senderProcess.on('close', (code, signal) => {
                log.error('Sender', 'Synchronizer process exited with code %s signal %s', code, signal);
            });
        });
}

function scheduleCheck() {
    senderProcess.send({
        type: 'schedule-check',
        tid: messageTid
    });

    messageTid++;
}

module.exports.spawn = bluebird.promisify(spawn);
module.exports.scheduleCheck = scheduleCheck;
