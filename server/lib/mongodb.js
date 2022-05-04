'use strict';

const config = require('./config');
const log = require('./log');
const { MongoClient } = require('mongodb');

/**
 * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
 * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
 */
let mongodb = null;

module.exports = async () => {
    if (mongodb !== null)
        return mongodb;

    try {
        log.info('MongoDB', 'Connecting to MongoDB cluster...');
        const uri = config.mongodb.uri;
        const mongoDBClient = new MongoClient(uri);

        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        log.info('MongoDB', 'Successfully connected to MongoDB cluster!');
        /* Return mailtrain database */
        return mongoDBClient.db('mailtrain');
    } catch (error) {
        log.verbose('MongoDB', error);
    }
};
