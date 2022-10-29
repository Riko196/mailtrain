'use strict';

const config = require('../../config');
const { getMongoDB } = require('../../mongodb');

/** It defines range of e-mail hash values according to which the workers divide their messages for sending. */
const MAX_RANGE = config.sender.maxRange;

/** It defines all possible states in which one worker can appear. */
const SenderWorkerState = {
    SYNCHRONIZING: 0,
    WORKING: 1,
    DEAD: 2
};

/**
 * Reset sender_workers collection.
 */
 async function resetSenderWorkersCollection() {
    await getMongoDB().collection('sender_workers').deleteMany({});

    for (let id = 0; id < config.sender.workers; id++) {
        await getMongoDB().collection('sender_workers').insertOne(computeSenderWorkerInit(id, SenderWorkerState.SYNCHRONIZING));
    }
};

/**
 * Compute SenderWorker init values.
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
 * Get init SenderWorker if synchronized is set.
 */
async function senderWorkerSynchronizedInit(workerId, maxWorkers) {
    /* Setup SYNCHRONIZING state and report first alive state */
    await getMongoDB().collection('sender_workers').updateOne({ _id: workerId },
                { $set: { lastReport: new Date(), state: SenderWorkerState.SYNCHRONIZING  } });

    const existingSenderWorker = await getMongoDB().collection('sender_workers').findOne({ _id: workerId });
    return { ...existingSenderWorker, maxWorkers };
};

/**
 * Get init SenderWorker if synchronized is not set.
 */
function senderWorkerInit(workerId, maxWorkers) {
    const init = computeSenderWorkerInit(workerId, SenderWorkerState.WORKING);
    return { ...init, maxWorkers };
}

module.exports.SenderWorkerState = SenderWorkerState;
module.exports.resetSenderWorkersCollection = resetSenderWorkersCollection;
module.exports.senderWorkerSynchronizedInit = senderWorkerSynchronizedInit;
module.exports.senderWorkerInit = senderWorkerInit;
