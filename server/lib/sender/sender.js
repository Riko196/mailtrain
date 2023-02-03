'use strict';

const path = require('path');
const bluebird = require('bluebird');
const config = require('../config');
const fork = require('../fork').fork;
const knex = require('../knex');
const log = require('../log');
const builtinZoneMta = require('../builtin-zone-mta');
const { CampaignStatus } = require('../../../shared/campaigns');
const { initSenderWorkersCollection } = require('./sender-worker/init');

let messageTid = 0;
let synchronizerProcess;

/**
 *  Setup synchronizing camapaigns to scheduled status again and spawn Synchronizer.
 */
async function spawnSynchronizer() {
    log.verbose('Sender', 'Spawning synchronizer process');

    await knex('campaigns').where('status', CampaignStatus.SYNCHRONIZING).update({ status: CampaignStatus.SCHEDULED });

    return await new Promise((resolve, reject) => {
        synchronizerProcess = fork(path.join(__dirname, '..', '..', 'services', 'synchronizer.js'), [], {
            cwd: path.join(__dirname, '..', '..'),
            env: {
                NODE_ENV: process.env.NODE_ENV,
                BUILTIN_ZONE_MTA_PASSWORD: builtinZoneMta.getPassword()
            }
        });

        synchronizerProcess.on('message', msg => {
            if (msg) {
                if (msg.type === 'synchronizer-started') {
                    log.info('Sender', 'Synchronizer started!');
                    return resolve();
                }
            }
        });

        synchronizerProcess.on('close', (code, signal) => {
            log.error('Sender', 'Synchronizer process exited with code %s signal %s', code, signal);
            return reject();
        });
    });
}

/**
 *  Callback, used as an immediate schedule check by QueryExecutor.
 */
function scheduleCheck() {
    synchronizerProcess.send({
        type: 'schedule-check',
        tid: messageTid
    });

    messageTid++;
}

/**
 * Spawn sender worker.
 * 
 * @argument workerId - Id of worker for spawning
 * 
 * @returns promise result of spawning
 */
async function spawnWorker(workerId) {
    return await new Promise((resolve, reject) => {
        log.verbose('Sender', `Spawning worker process ${workerId}`);

        const workerProcess = fork(path.join(__dirname, '..', '..', 'services', 'sender-worker.js'), [], {
            cwd: path.join(__dirname, '..', '..'),
            env: {
                NODE_ENV: process.env.NODE_ENV,
                BUILTIN_ZONE_MTA_PASSWORD: builtinZoneMta.getPassword(),
                WORKER_ID: workerId
            }
        });

        workerProcess.on('message', msg => {
            if (msg) {
                if (msg.type === 'worker-started') {
                    log.info('Sender', `Worker with ID ${workerId} started!`);
                    return resolve();
                } 
            }
        });

        workerProcess.on('close', (code, signal) => {
            log.error('Sender', `Worker process ${workerId} exited with code %s signal %s`, code, signal);
            return reject();
        });
    });
}

/**
 * Spawn Sender component (Synchronizer + all Workers).
 * 
 * @argument callback - function that represents bluebird.promisify
 * 
 * @returns callback result 
 */
async function spawn(callback) {
    await spawnSynchronizer();

    /* 
     * Init sender_workers collection before spawning if it has not been initialized yet 
     * (it has impact iff workerSynchronization is set, otherwise it is redundant call)
     */
    await initSenderWorkersCollection();

    /* Spawn all sender workers if mailtrain is in centralized mode */
    if (config.mode === 'centralized') {
        const spawnWorkerFutures = [];

        for (let workerId = 0; workerId < config.sender.workers; workerId++) {
            spawnWorkerFutures.push(spawnWorker(workerId));
        }

        /* Wait until every worker starts */
        await Promise.all(spawnWorkerFutures);
    }

    return callback();
}

module.exports.spawn = bluebird.promisify(spawn);
module.exports.scheduleCheck = scheduleCheck;
