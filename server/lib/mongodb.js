'use strict';

const config = require('./config');
const log = require('./log');
const { MongoClient } = require('mongodb');

/**
 * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
 * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
 */

let mongodb = null;

async function connectToMongoDB() {
    try {
        log.info('MongoDB', 'Connecting to MongoDB cluster...');
        const uri = config.mongodb.uri;
        const mongoDBClient = new MongoClient(uri);

        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        log.info('MongoDB', 'Successfully connected to MongoDB cluster!');
        /* Return mailtrain database */
        mongodb = mongoDBClient.db('mailtrain');
    } catch (error) {
        log.error('MongoDB', error);
    }
}

function getMongoDB() {
    return mongodb;
}

module.exports.connectToMongoDB = connectToMongoDB;
module.exports.getMongoDB = getMongoDB;
