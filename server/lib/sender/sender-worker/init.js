'use strict';

const config = require('../../config');
const log = require('../../log');
const { getMongoDB, getNewTransactionSession, transactionOptions } = require('../../mongodb');
const PlatformSolver = require('../platform-solver');

/** Get number of all workers (it is taken from different variable according to running platform) */
const MAX_WORKERS = PlatformSolver.getNumberOfWorkers();
/** It defines range of e-mail hash values according to which the workers divide their messages for sending. */
const MAX_RANGE = config.sender.maxRange;

/** It defines all possible states in which one worker can appear. */
const SenderWorkerState = {
    SYNCHRONIZING: 0,
    WORKING: 1,
    DEAD: 2
};

async function senderWorkerInit() {
    /* Computing WorkerID and defined hash range of his campaign_messages */
    const workerId = PlatformSolver.getWorkerId();

    /* Check whether this sender worker already exists in the collection */
    const existingSenderWorker = await getMongoDB().collection('sender_workers').findOne({ _id: workerId });
    if (existingSenderWorker) {
        /* Setup SYNCHRONIZING state and report first alive state */
        const transactionSession = getNewTransactionSession();
        await transactionSession.withTransaction(async () => {
            const updateResult = await getMongoDB().collection('sender_workers').updateOne({ _id: workerId },
                    { $set: { lastReport: new Date(), state: SenderWorkerState.SYNCHRONIZING  } }, { transactionSession });

            if (updateResult.modifiedCount) {
                log.info(`SenderWorker:${workerId}`, `Worker successfully set to SYNCHRONIZING state!`);
            } else {
                log.error(`SenderWorker:${workerId}`, `Worker failed set to SYNCHRONIZING state!`);
            }
        }, transactionOptions);
        await transactionSession.endSession();
        return { ...existingSenderWorker, maxWorkers: MAX_WORKERS };
    }
    /* Computing hash range of his campaign_messages for which he is responsible to send */
    const range = {
        from: Math.floor(MAX_RANGE / MAX_WORKERS)  * workerId,
        to: Math.floor(MAX_RANGE / MAX_WORKERS)  * (workerId + 1)
    }
    /* If it is the last worker then assign MAX_RANGE */
    if (workerId === MAX_WORKERS - 1) {
        range.to = MAX_RANGE;
    }

    const senderWorkerInitState = {
        _id: workerId,
        state: SenderWorkerState.SYNCHRONIZING,
        range,
        lastReport: new Date(),
        substitute: null
    };

    /* Insert SenderWorker with init state */
    await getMongoDB().collection('sender_workers').insertOne(senderWorkerInitState);
    return { ...senderWorkerInitState, maxWorkers: MAX_WORKERS };
};

module.exports.SenderWorkerState = SenderWorkerState;
module.exports.senderWorkerInit = senderWorkerInit;
