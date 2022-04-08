const { MongoClient } = require('mongodb');
const config = require('../../lib/config');

/**
 * The main component of distributed system for sending email.
 */
 class SenderNode {
    constructor() {
        this.client = {};
    }

    async senderNodeLoop() {
        const uri = config.mongodb.uri;
        this.client = new MongoClient(uri);

        try {
            // Connect to the MongoDB cluster
            await this.client.connect();

            // Make the appropriate DB calls
            setInterval(async () => {
                await this.listDatabases();
            }, 5000);
        } catch (e) {
            console.error(e);
        }
    }

    async listDatabases(){
        const databasesList = await this.client.db().admin().listDatabases();

        console.log("Databases:");
        databasesList.databases.forEach(db => console.log(` - ${db.name}`));
    };
}

new SenderNode().senderNodeLoop();
