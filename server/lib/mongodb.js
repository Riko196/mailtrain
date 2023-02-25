'use strict';

const knex = require('./knex');
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
        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        /* Return mailtrain database */
        mongodb = mongoDBClient.db('mailtrain');
    } catch (error) {
        log.error('MongoDB', error);
    }
}

async function dropMailtrainMongoDB() {
    try {
        /* Connect to the MongoDB cluster */
        await mongoDBClient.connect();

        /* Take mailtrain database */
        mongodb = mongoDBClient.db('mailtrain');

        /* Drop mailtrain datbase */
        await mongodb.dropDatabase();

        await mongoDBClient.close();
    } catch (error) {
        log.error('MongoDB', error);
    }
}

function getNewTransactionSession() {
    return mongoDBClient.startSession();
}

async function knexMongoDBTransaction(callback) {
    const mongoDBSession = getNewTransactionSession();

    try {
        let transactionResult = null;
        
        await mongoDBSession.withTransaction(async () => {
            await knex.transaction(async knexTx => {
                transactionResult = await callback(knexTx, mongoDBSession);

                if (mongoDBSession.transaction.state === 'TRANSACTION_ABORTED') {
                    await knexTx.rollback(new Error("Transaction ABORTED! Try again."));
                }
            });

            /* Knex transaction always throws exception if is aborted so here we do not need to check it manually */
        }, transactionOptions);

        return transactionResult;
    } catch (error) {
        throw error;
    } finally {
        await mongoDBSession.endSession();
    }
}

function getMongoDB() {
    return mongodb;
}

module.exports.transactionOptions = transactionOptions;
module.exports.connectToMongoDB = connectToMongoDB;
module.exports.getNewTransactionSession = getNewTransactionSession;
module.exports.dropMailtrainMongoDB = dropMailtrainMongoDB;
module.exports.getMongoDB = getMongoDB;
module.exports.knexMongoDBTransaction = knexMongoDBTransaction;
