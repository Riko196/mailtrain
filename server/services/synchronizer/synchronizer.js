
/** 
 * The main component for synchronizing between non-high-available centralized and high-available distributed components. It initializes scheduler and 
 * data collector and then in loop it communicates with MongoDB database. It takes data from DataCollector and then sends them
 * to MongoDB database at once for ensuring high-availability.
 */
class Synchronizer {
    constructor() {
        this.synchronizingCampaigns = [];
        this.dataCollector = new DataCollector();
        this.scheduler = new Scheduler();
        setImmediate(this.synchronizerLoop);
    }

    async synchronizerLoop() {
        async function getSynchronizingCampaign() {
            while (this.synchronizingCampaigns.length === 0) {
                await notifier.waitFor('workerFinished');
            }
    
            return this.synchronizingCampaigns.shift();
        }

        function selectNextTask() {
            const allocationMap = new Map();
            const allocation = [];
    
            function initAllocation(waType, attrName, queues, workerMsg, getSendConfigurationId, getQueueEmptyEvent) {
                for (const id of queues.keys()) {
                    const sendConfigurationId = getSendConfigurationId(id);
                    const key = attrName + ':' + id;
    
                    const queue = queues.get(id);
    
                    const postponed = isSendConfigurationPostponed(sendConfigurationId);
    
                    const task = {
                        type: waType,
                        id,
                        existingWorkers: 0,
                        isValid: queue.length > 0 && !postponed,
                        queue,
                        workerMsg,
                        attrName,
                        getQueueEmptyEvent,
                        sendConfigurationId
                    };
    
                    allocationMap.set(key, task);
                    allocation.push(task);
    
                    if (postponed && queue.length > 0) {
                        queue.splice(0);
                        notifier.notify(task.getQueueEmptyEvent(task));
                    }
                }
    
                for (const wa of workAssignment.values()) {
                    if (wa.type === waType) {
                        const key = attrName + ':' + wa[attrName];
                        const task = allocationMap.get(key);
                        task.existingWorkers += 1;
                    }
                }
            }
    
            initAllocation(
                WorkAssignmentType.QUEUED,
                'sendConfigurationId',
                sendConfigurationMessageQueue,
                'process-queued-messages',
                id => id,
                task => `sendConfigurationMessageQueueEmpty:${task.id}`
            );
    
            initAllocation(
                WorkAssignmentType.CAMPAIGN,
                'campaignId',
                campaignMessageQueue,
                'process-campaign-messages',
                id => sendConfigurationIdByCampaignId.get(id),
                task => `campaignMessageQueueEmpty:${task.id}`
            );
    
            let minTask = null;
            let minExistingWorkers;
    
            for (const task of allocation) {
                if (task.isValid && (minTask === null || minExistingWorkers > task.existingWorkers)) {
                    minTask = task;
                    minExistingWorkers = task.existingWorkers;
                }
            }
    
            return minTask;
        }
    
    
        while (true) {
            const campaignId = await this.getSynchronizingCampaign();
    
            if (campaignId) {
                const data = this.dataCollector.collectData(campaignId);

                await this.sendDataToMongo(data);
            } else {
                await notifier.waitFor('workAvailable');
            }
        }
    }

    async sendDataToMongo(data) {

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