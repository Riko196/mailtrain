'use strict';

const config = require('../../config');
const log = require('../../log');
const { getMongoDB, getNewTransactionSession, transactionOptions } = require('../../mongodb');

/** It defines range of e-mail hash values according to which the workers divide their messages for sending. */
const MAX_RANGE = config.sender.maxRange;

/** It defines all possible states in which one worker can appear. */
const SenderWorkerState = {
    SYNCHRONIZING: 0,
    WORKING: 1,
    DEAD: 2
};

/**
 * @returns whether the sender workers will synchronize with each other.
 */
function workerSynchronizationIsSet() {
    return config.sender.workerSynchronization;
}

/**
 * Init sender_workers collection if it has not been initialized yet.
 */
async function initSenderWorkersCollection() {
    const maxWorkers = config.sender.workers;
    const countOfWorkers = await getMongoDB().collection('sender_workers').countDocuments({});

    if (countOfWorkers !== maxWorkers) {
        log.info('Sender', 'Initializing sender_workers collection...');
        await getMongoDB().collection('sender_workers').deleteMany({});
        for (let id = 0; id < maxWorkers; id++) {
            await getMongoDB().collection('sender_workers').insertOne(computeSenderWorkerInit(id, SenderWorkerState.SYNCHRONIZING, maxWorkers));
        }
    }
}

/**
 * Compute SenderWorker init values.
 * 
 * @argument workerId - Id of SenderWorker
 * @argument initState - Init state of SenderWorker
 * @argument maxWorkers - maximum number of SenderWorkers
 * @returns initialized SenderWorker object with the current and all needed data.
 */
function computeSenderWorkerInit(workerId, initState, maxWorkers) {
    /* Computing hash range of his campaign_messages for which he is responsible to send */
    const range = {
        from: Math.floor(MAX_RANGE / maxWorkers)  * workerId,
        to: Math.floor(MAX_RANGE / maxWorkers)  * (workerId + 1)
    }
    /* If it is the last worker then assign MAX_RANGE */
    if (workerId === maxWorkers - 1) {
        range.to = MAX_RANGE;
    }

    return {
        _id: workerId,
        state: initState,
        range,
        lastReport: new Date(),
        substitute: null
    };
}

/**
 * Get SenderWorker to the SYNCHRONIZING state.
 * 
 * @argument workerId - Id of SenderWorker
 */
async function goToSynchronizingState(workerId) {
    const transactionSession = getNewTransactionSession();

    let successfulTransaction = false;
    while (!successfulTransaction) {
        successfulTransaction = true;
        try {
            await transactionSession.withTransaction(async () => {
                /* Setup SYNCHRONIZING state and report first alive state */
                await getMongoDB().collection('sender_workers').updateOne(
                    { _id: workerId },
                    { $set: { lastReport: new Date(), state: SenderWorkerState.SYNCHRONIZING } },
                    { session: transactionSession }
                );
        
                /* Setup null substitute for all workers still substituted by this worker
                 * (it could happen when worker substitutes someone and is turned off and
                 * turned on too fast and another worker has not enough time to set it up) */
                await getMongoDB().collection('sender_workers').updateMany(
                    { substitute: workerId },
                    { $set: { substitute: null } },
                    { session: transactionSession }
                );
            }, transactionOptions);
        } catch(error) {
            successfulTransaction = false;
        } finally {
            await transactionSession.endSession();
        }
    }
}

/**
 * Get SenderWorker data if synchronized is set.
 * 
 * @argument workerId - Id of SenderWorker
 * @returns SenderWorker object with the current and all needed data.
 */
async function senderWorkerSynchronizedInit(workerId) {
    await goToSynchronizingState(workerId);
    const existingSenderWorker = await getMongoDB().collection('sender_workers').findOne({ _id: workerId });
    return existingSenderWorker;
}

/**
 * Get SenderWorker data.
 * 
 * @argument workerId - Id of SenderWorker
 * @argument maxWorkers - maximum number of SenderWorkers
 * @returns SenderWorker object with the current and all needed data.
 */
async function senderWorkerInit(workerId, maxWorkers) {
    let senderWorker = null;
    
    if (workerSynchronizationIsSet()) {
        senderWorker = await senderWorkerSynchronizedInit(workerId, maxWorkers);
    } else {
        senderWorker = computeSenderWorkerInit(workerId, SenderWorkerState.WORKING, maxWorkers);
    }

    return { ...senderWorker, maxWorkers };
}

module.exports.SenderWorkerState = SenderWorkerState;
module.exports.workerSynchronizationIsSet = workerSynchronizationIsSet;
module.exports.goToSynchronizingState = goToSynchronizingState;
module.exports.initSenderWorkersCollection = initSenderWorkersCollection;
module.exports.senderWorkerSynchronizedInit = senderWorkerSynchronizedInit;
module.exports.senderWorkerInit = senderWorkerInit;
