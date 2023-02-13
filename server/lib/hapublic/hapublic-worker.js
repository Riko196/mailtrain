const { connectToMongoDB } = require('../mongodb');
const { AppType } = require('../../../shared/app');
const appBuilder = require('../../app-builder');
const { startHTTPServer } = require('../http-server');
const config = require('../config');

/* The port on which will HAPUBLIC worker run. */
const workerPort = process.env.HAPUBLIC_WORKER_PORT;

/* Spawn HAPUBLIC worker which represents HTTP unit server managed by HAProxy for sending linked files. */
async function spawnHapublicWorker() {
    /* Connect to the MongoDB and accomplish setup */
    await connectToMongoDB();
    
    /* Start HAPUBLIC worker */
    await startHTTPServer(AppType.HAPUBLIC, 'hapublic', workerPort);

    if (config.title) {
        process.title = config.title + ': haPublicWorker';
    }

    if (config.mode === 'centralized') {
        process.send({
            type: 'haPublicWorker-started'
        });
    }

    appBuilder.setReady();
}

/* noinspection JSIgnoredPromiseFromCall */
spawnHapublicWorker();
