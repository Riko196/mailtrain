'use strict';

const config = require('./config');
const log = require('./log');
const { MongoClient } = require('mongodb');

/**
 * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
 * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
 */

const uri = process.env.SLURM_MONGODB_URL ? process.env.SLURM_MONGODB_URL : config.mongodb.uri;
const mongoDBClient = new MongoClient(uri);
let mongodb = null;

const transactionOptions = {
    readPreference: 'primary',
    readConcern: { level: 'local' },
    writeConcern: { w: 'majority' }
};

async function connectToMongoDB() {
    try {
        log.info('MongoDB', 'Connecting to MongoDB cluster...');

        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        log.info('MongoDB', 'Successfully connected to MongoDB cluster!');
        /* Return mailtrain database */
        mongodb = mongoDBClient.db('mailtrain');
    } catch (error) {
        log.error('MongoDB', error);
    }
}

function getNewSession() {
    return mongoDBClient.startSession();
}

function getMongoDB() {
    return mongodb;
}

module.exports.transactionOptions = transactionOptions;
module.exports.connectToMongoDB = connectToMongoDB;
module.exports.getNewSession = getNewSession;
module.exports.getMongoDB = getMongoDB;
