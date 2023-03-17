const knex = require('../../lib/knex');
const { mongodbCheck } = require('../../lib/dbcheck');
const log = require('../../lib/log');
const subscriptions = require('../../models/subscriptions');
const { dropMailtrainMongoDB, connectToMongoDB, getMongoDB } = require('../../lib/mongodb');

/**
 *  Async function which synchronizes whole MySQL and MongoDB databases. It removes all collections from MongoDB
 *  and then sends all needed tables from MySQL to MongoDB.
 */
async function synchronizeMongoDbWithMySQL() {
    try {
        /* Drop the whole database */
        log.info('Syncer', 'Dropping database...');
        await dropMailtrainMongoDB();

        /* Rebuild mailtrain MongoDB database */
        await mongodbCheck();

        /* Reconnecting after the database was dropped */
        log.info('Syncer', 'Reconnecting to mailtrain MongoDB database...');
        await connectToMongoDB();
        const mongodb = getMongoDB();
        log.info('Syncer', 'Successfully reconnected to mailtrain MongoDB database!');

        await synchronizeSymmetricReplicationCollections(mongodb);
        await synchronizeQueuedCollections(mongodb);

        log.info('Syncer', 'MongoDB database successfully synchronized with MySQL database!');
    } catch(error) {
        log.error('Syncer', 'Error: ', error);
    }

    process.exit();
}

/**
 *  Async function which synchronizes all MySQL tables with MongoDB collections that represent queued collections.
 */
async function synchronizeQueuedCollections(mongodb) {
    try {
        /* Synchronizing campaign_messages */
        log.info('Initializer', 'Synchronizing campaign_messages collection...');
        const campaignMessages = await knex('campaign_messages').select('*');
        if (campaignMessages.length) {
            campaignMessages.map(campaignMessage => {
                campaignMessage._id = campaignMessage.id;
                delete campaignMessage.id;
            });

            await mongodb.collection('campaign_messages').insertMany(campaignMessages);
        }
    } catch(error) {
        log.error('Initializer', 'Error: ', error);
    }
}

/**
 *  Async function which synchronizes all MySQL tables with MongoDB collections that represent symmetric replications.
 */
async function synchronizeSymmetricReplicationCollections(mongodb) {
    try {
        /* Synchronizing blacklist */
        log.info('Initializer', 'Synchronizing blacklist collection...');
        const blackSubscribers = await knex('blacklist').select('*');
        if (blackSubscribers.length) {
            await mongodb.collection('blacklist').insertMany(blackSubscribers);
        }

        /* Synchronizing subscriptions */
        log.info('Initializer', 'Synchronizing lists collections...');
        const listIDs = await knex('lists').select('id');
        for (const listID of listIDs) {
            const subscribers = await knex(subscriptions.getSubscriptionTableName(listID.id)).select('*');

            log.info('Initializer', `Synchronizing ${subscriptions.getSubscriptionTableName(listID.id)} collection...`);

            subscribers.map(subscriber => {
                subscriber._id = subscriber.id;
                delete subscriber.id;
            });

            if (subscribers.length) {
                await mongodb.collection(subscriptions.getSubscriptionTableName(listID.id)).insertMany(subscribers);
            }
        }

        /* Synchronizing files_campaign_file */
        log.info('Initializer', 'Synchronizing files_campaign_file collection...');
        const filesCampaignFile = await knex('files_campaign_file').select('*');
        if (filesCampaignFile.length) {
            filesCampaignFile.map(file => {
                file._id = file.id;
                delete file.id;
            });
            await mongodb.collection('files_campaign_file').insertMany(filesCampaignFile);
        }
        
        log.info('Initializer', 'MongoDB database successfully synchronized with MySQL database!');
    } catch(error) {
        log.error('Initializer', 'Error: ', error);
    }
}

synchronizeMongoDbWithMySQL();
