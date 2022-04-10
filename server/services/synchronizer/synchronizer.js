
/**
 * The main component for synchronizing between non-high-available centralized and high-available distributed components. It initializes scheduler and
 * data collector and then in loop it communicates with MongoDB database. It takes data from DataCollector and then sends them
 * to MongoDB database at once for ensuring high-availability.
 */
class Synchronizer {
    constructor() {
        this.synchronizingCampaigns = [];
        this.notifier = new Notifier();
        /* sendConfigurationId -> {retryCount, postponeTill} */
        this.sendConfigurationStatuses = new Map();
        this.dataCollector = new DataCollector();
        this.scheduler = new Scheduler(this.synchronizingCampaigns, this.notifier, this.sendConfigurationStatuses);
        setImmediate(this.synchronizerLoop);
    }

    async synchronizerLoop() {
        async function selectNextTask() {
            while (this.synchronizingCampaigns.length === 0) {
                await notifier.waitFor('workerFinished');
            }

            return this.synchronizingCampaigns.shift();
        }


        while (true) {
            const campaignId = await this.getSynchronizingCampaign();

            if (campaignId) {
                const data = this.dataCollector.collectData(campaignId);

                await this.sendDataToMongoDB(data);
            } else {
                await notifier.waitFor('taskAvailable');
            }
        }
    }

    async sendDataToMongoDB(data) {

    }

    isSendConfigurationPostponed(sendConfigurationId) {
        const now = Date.now();
        const sendConfigurationStatus = getSendConfigurationStatus(sendConfigurationId);
        return sendConfigurationStatus.postponeTill > now;
    }

    getSendConfigurationStatus(sendConfigurationId) {
        let status = sendConfigurationStatuses.get(sendConfigurationId);
        if (!status) {
            status = {
                retryCount: 0,
                postponeTill: 0
            };

            sendConfigurationStatuses.set(sendConfigurationId, status);
        }

        return status;
    }
}

/* Inner class for Synchronizer which notifies processes about  */
class Notifier {
    constructor() {
        this.conts = new Map();
    }

    notify(id) {
        const cont = this.conts.get(id);
        if (cont) {
            for (const cb of cont) {
                setImmediate(cb);
            }
            this.conts.delete(id);
        }
    }

    async waitFor(id) {
        let cont = this.conts.get(id);
        if (!cont) {
            cont = [];
        }

        const notified = new Promise(resolve => {
            cont.push(resolve);
        });

        this.conts.set(id, cont);

        await notified;
    }
}
