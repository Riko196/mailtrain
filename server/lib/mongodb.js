'use strict';

const config = require('./config');
const log = require('./log');
const { MongoClient } = require('mongodb');

/**
 * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
 * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
 */
let mongodb = null;

const init = async () => {
    try {
        log.verbose('MongoDB', 'Connecting to MongoDB cluster...');
        const uri = config.mongodb;
        const mongoDBClient = new MongoClient(uri);
        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        /* Return mailtrain database */
        mongodb = mongoDBClient.db('mailtrain');
        log.verbose('MongoDB', 'Successfully connected to MongoDB cluster!');
    } catch (error) {
        log.verbose('MongoDB', error);
    }
};

/* Export mailtrain database */
module.exports = mongodb;
