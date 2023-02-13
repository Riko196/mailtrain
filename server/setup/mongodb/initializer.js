const knex = require('../../lib/knex');
const { mongodbCheck } = require('../../lib/dbcheck');
const log = require('../../lib/log');
const { dropMailtrainMongoDB, connectToMongoDB, getMongoDB } = require('../../lib/mongodb');
const subscriptions = require('../../models/subscriptions');

/**
 *  Async function which synchronizes whole MySQL and MongoDB databases. It removes all collections from MongoDB
 *  and then sends all needed tables from MySQL to MongoDB.
 */
async function synchronizeMongoDbWithMySQL() {
    try {
        /* Drop the whole database */
        log.info('Synchronizer', 'Dropping database...');
        await dropMailtrainMongoDB();

        /* Rebuild mailtrain MongoDB database */
        await mongodbCheck();

        /* Reconnecting after the database was dropped */
        log.info('Synchronizer', 'Reconnecting to mailtrain MongoDB database...');
        await connectToMongoDB();
        const mongodb = getMongoDB();
        log.info('Synchronizer', 'Successfully reconnected to mailtrain MongoDB database!');

        /* Synchronizing blacklist */
        log.info('Synchronizer', 'Synchronizing blacklist collection...');
        const blackSubscribers = await knex('blacklist').select('*');
        if (blackSubscribers.length) {
            await mongodb.collection('blacklist').insertMany(blackSubscribers);
        }

        /* Synchronizing campaign_messages */
        log.info('Synchronizer', 'Synchronizing campaign_messages collection...');
        const campaignMessages = await knex('campaign_messages').select('*');
        if (campaignMessages.length) {
            campaignMessages.map(campaignMessage => {
                campaignMessage._id = campaignMessage.id;
                delete campaignMessage.id;
            });

            await mongodb.collection('campaign_messages').insertMany(campaignMessages);
        }

        /* Synchronizing subscriptions */
        log.info('Synchronizer', 'Synchronizing lists collections...');
        const listIDs = await knex('lists').select('id');
        for (const listID of listIDs) {
            const subscribers = await knex(subscriptions.getSubscriptionTableName(listID.id)).select('*');

            log.info('Synchronizer', `Synchronizing ${subscriptions.getSubscriptionTableName(listID.id)} collection...`);

            subscribers.map(subscriber => {
                subscriber._id = subscriber.id;
                delete subscriber.id;
            });

            if (subscribers.length) {
                await mongodb.collection(subscriptions.getSubscriptionTableName(listID.id)).insertMany(subscribers);
            }
        }

        /* Synchronizing files_campaign_file */
        log.info('Synchronizer', 'Synchronizing files_campaign_file collection...');
        const filesCampaignFile = await knex('files_campaign_file').select('*');
        if (filesCampaignFile.length) {
            filesCampaignFile.map(file => {
                file._id = file.id;
                delete file.id;
            });
            await mongodb.collection('files_campaign_file').insertMany(filesCampaignFile);
        }
        
        log.info('Synchronizer', 'MongoDB database successfully synchronized with MySQL database!');
    } catch(error) {
        log.error('Synchronizer', 'Error: ', error);
        console.log(error);
    }
    process.exit();
}

synchronizeMongoDbWithMySQL();
