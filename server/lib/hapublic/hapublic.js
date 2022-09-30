'use strict';

const path = require('path');
const bluebird = require('bluebird');
const config = require('../config');
const fork = require('../fork').fork;
const log = require('../log');

const haPublicPorts = config.www.haPublicPorts;

/* Spawn HAPUBLIC worker */
async function spawnWorker(workerPort) {
    return await new Promise((resolve, reject) => {
        log.verbose('HAPUBLIC', `Spawning worker process ${workerPort}`);

        const workerProcess = fork(path.join(__dirname, '..', '..', 'services', 'hapublic-worker.js'), [], {
            cwd: path.join(__dirname, '..', '..'),
            env: {
                NODE_ENV: process.env.NODE_ENV,
                WORKER_PORT: workerPort
            }
        });

        workerProcess.on('message', msg => {
            if (msg) {
                if (msg.type === 'haPublicWorker-started') {
                    return resolve();
                } 
            }
        });

        workerProcess.on('close', (code, signal) => {
            return reject();
        });
    });
}

/* Spawn HAPUBLIC workers (HAProxy daemon is run during Mailtrain installation) */
async function spawn(callback) {
    if (config.mode === 'centralized') {
        /* Start n HAPUBLIC servers which are controlled by HAProxy process */
        for (const haPublicPort of haPublicPorts) {
            await spawnWorker(haPublicPort);
        }
    }

    return callback();
}

module.exports.spawn = bluebird.promisify(spawn);
