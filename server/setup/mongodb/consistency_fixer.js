const knex = require('../../lib/knex');
const log = require('../../lib/log');
const subscriptions = require('../../models/subscriptions');
const { connectToMongoDB, getMongoDB } = require('../../lib/mongodb');

async function fixDataConsistencyInMongoDB() {
    try {
        await connectToMongoDB();
        let queries = [];

        /* Fixing data in blacklist */
        log.info('ConsistencyFixer', 'Fixing data in blacklist collection...');
        const blackSubscribers = await knex('blacklist').select('*');
        blackSubscribers.forEach(blackSubscriber => {
            const query = {
                updateOne: {
                    filter: { email: blackSubscriber.email },
                    update: {
                        $set: {},
                        $setOnInsert: blackSubscriber
                    },
                    upsert: true
                }
            };
            queries.push(query);
        })

        if (blackSubscribers.length) {
            await getMongoDB().collection('blacklist').bulkWrite(queries, { ordered: false });
        }
        /* Fixing data in subscriptions */
        log.info('ConsistencyFixer', 'Fixing data in lists collections...');
        const listIDs = await knex('lists').select('id');
        for (const listID of listIDs) {
            const subscribers = await knex(subscriptions.getSubscriptionTableName(listID.id)).select('*');

            log.info('ConsistencyFixer', `Fixing data in ${subscriptions.getSubscriptionTableName(listID.id)} collection...`);

            subscribers.map(subscriber => {
                subscriber._id = subscriber.id;
                delete subscriber.id;
            });

            if (subscribers.length) {
                queries = [];
                subscribers.forEach(subscriber => {
                    const query = {
                        updateOne: {
                            filter: { _id: subscriber._id },
                            update: {
                                $set: {},
                                $setOnInsert: subscriber
                            },
                            upsert: true
                        }
                    };
                    queries.push(query);
                })
                await getMongoDB().collection(subscriptions.getSubscriptionTableName(listID.id)).bulkWrite(queries, { ordered: false });
            }
        }

        /* Fixing data in files_campaign_file */
        log.info('ConsistencyFixer', 'Fixing data in files_campaign_file collection...');
        const filesCampaignFile = await knex('files_campaign_file').select('*');
        if (filesCampaignFile.length) {
            filesCampaignFile.map(file => {
                file._id = file.id;
                delete file.id;
            });

            queries = [];
            filesCampaignFile.forEach(file => {
                const query = {
                    updateOne: {
                        filter: { _id: file._id },
                        update: {
                            $set: {},
                            $setOnInsert: file
                        },
                        upsert: true
                    }
                };
                queries.push(query);
            })

            await getMongoDB().collection('files_campaign_file').bulkWrite(queries, { ordered: false });
        }
        
        log.info('ConsistencyFixer', 'MongoDB database successfully synchronized with MySQL database!');
    } catch(error) {
        log.error('ConsistencyFixer', 'Error: ', error);
    }

    process.exit();
}

fixDataConsistencyInMongoDB();
