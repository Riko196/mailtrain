const knex = require('../../lib/knex');
const { connectToMongoDB, getMongoDB } = require('../../lib/mongodb');
const subscriptions = require('../../models/subscriptions');

/*
    Async function which synchronizes whole MySQL and MongoDB databases. It removes all collections from MongoDB
    and then sends all needed tables from MySQL to MongoDB.
*/
async function synchronizeMongoDbWithMySQL() {
    try {
        await connectToMongoDB();
        let mongodb = getMongoDB();

        /* Drop all collections */
        const collections = await mongodb.listCollections().toArray();
        for (const collection of collections) {
            if (collection.name !== 'tasks') {
                await mongodb.collection(collection.name).drop();
            }
        }

        /* Reconnecting after the database was dropped */
        await connectToMongoDB();
        mongodb = getMongoDB();

        /* Synchronizing blacklist */
        const blackSubscribers = await knex('blacklist').select('*');
        if (blackSubscribers.length) {
            await mongodb.collection('blacklist').insertMany(blackSubscribers);
        }

        /* Synchronizing campaign_messages */
        const campaignMessages = await knex('campaign_messages').select('*');
        if (campaignMessages.length) {
            campaignMessages.map(campaignMessage => {
                campaignMessage._id = campaignMessage.id;
                delete campaignMessage.id;
            });

            await mongodb.collection('campaign_messages').insertMany(campaignMessages);
        }

        /* Synchronizing subscriptions */
        const listIDs = await knex('lists').select('id');
        for (const listID of listIDs) {
            const subscribers = await knex(subscriptions.getSubscriptionTableName(listID.id)).select('*');

            subscribers.map(subscriber => {
                subscriber._id = subscriber.id;
                delete subscriber.id;
            });

            if (subscribers.length) {
                await mongodb.collection(subscriptions.getSubscriptionTableName(listID.id)).insertMany(subscribers);
            }
        }
    } catch(error) {
        console.log(error);
    }
    process.exit(1);
}

synchronizeMongoDbWithMySQL();
