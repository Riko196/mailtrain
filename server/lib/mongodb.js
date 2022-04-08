'use strict';

const config = require('./config');
const log = require('./log');
const { MongoClient } = require('mongodb');

/**
 * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
 * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
 */
const uri = config.mongodb;
const client = new MongoClient(uri);

const init = async () => {
    try {
        log.verbose('MongoDB', 'Connecting to MongoDB cluster...');
        /* Connect to the MongoDB cluster */
        await client.connect();
        log.verbose('MongoDB', 'Successfully connected to MongoDB cluster!');
    } catch (error) {
        log.verbose('MongoDB', error);
    }
};

module.exports = { init, client };
