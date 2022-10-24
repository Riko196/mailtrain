'use strict';

const { getMongoDB, getNewTransactionSession, transactionOptions } = require('../../mongodb');
const { SenderWorkerState } = require('./init');
const log = require('../../log');

/** It defines period in which workers report alive state or synchronize with each other. */
const PERIOD = 15 * 1000;
/** It defines round of period in which workers synchronize with each other. */
const SYNCHRONIZING_ROUND = 5;

/**
 * SenderWorker component which periodically synchronize worker to which it belongs with all other workers.
 */
class WorkerSynchronizer {
    constructor(senderWorkerInfo, ranges) {
        /* Get number of maximum workers who can be spawned */
        this.maxWorkers = senderWorkerInfo.maxWorkers;
        /* Get WorkerID */
        this.workerId = senderWorkerInfo._id;
        /* Get the current worker state (used as a reference object) */
        this.state = { value: senderWorkerInfo.state };
        /* List of all ranges that this worker currently owns (his own range + ranges from non-working workers) */
        this.ranges = ranges;
        /* MongoDB session */
        this.mongodb = getMongoDB();
        /* Value that represents which round in order is performed (0 .. SYNCHRONIZING_ROUND possible values) */
        this.synchronizingNumber = 0;
        /* Start periodically synchronizing workers with each other */
        this.synchronizerPeriodicCheck();
    }

    /** 
     * In specified period, repeatedly report alive state and in every SYNCHRONIZING_ROUND round resolve dead workers without any substitution.
     */
    async synchronizerPeriodicCheck() {
        log.info(`SenderWorker:${this.workerId}`, `Worker INFO: ${this.state.value} ${JSON.stringify(this.ranges, null, 4)}`);
        const transactionSession = getNewTransactionSession();
        try {
            await this.reportAliveState(transactionSession);
            if (this.state.value === SenderWorkerState.WORKING && this.synchronizingNumber === SYNCHRONIZING_ROUND) {
                log.verbose(`SenderWorker:${this.workerId}`, 'SYNCHRONIZING ROUND...');
                await this.resolveDeadWorkers(transactionSession);
                await this.checkStateConsistency(transactionSession);
            } 
        } catch(error){
            await transactionSession.abortTransaction();
            log.error(`SenderWorker:${this.workerId}`, `Unexpected error occurred during synchronization: ${error}`);
            log.error(`SenderWorker:${this.workerId}`, error.stack);
        } finally {
            this.synchronizingNumber = this.synchronizingNumber === SYNCHRONIZING_ROUND ? 0 : this.synchronizingNumber + 1;    
            await transactionSession.endSession();
            setTimeout(this.synchronizerPeriodicCheck.bind(this), PERIOD);
        }
    }

    /** 
     * Write to the MongoDB collection that this worker is still alive.
     */
    async reportAliveState(transactionSession) {
        log.verbose(`SenderWorker:${this.workerId}`, 'Periodic report...');
        let updateResult = null;
        await transactionSession.withTransaction(async () => {
            updateResult = await this.mongodb.collection('sender_workers')
                .updateOne({ _id: this.workerId }, { $set: { lastReport: new Date() } }, { transactionSession });
        }, transactionOptions);
        if (!updateResult.modifiedCount) {
            log.error(`SenderWorker:${this.workerId}`, 'Missed reporting state!');
        }
    }

    /** 
     * Get all non-working workers (all substituted workers (SYNCHRONIZING or DEAD state)) or
     * non-working but not yet substituted (lastReport is too old (WORKING or DEAD state)). 
     */
    async getNonWorkingWorkers(transactionSession) {
        let nonworkingWorkers = [];
        await transactionSession.withTransaction(async () => {
            nonworkingWorkers = await this.mongodb.collection('sender_workers').aggregate([
                {
                    $addFields: {
                        reportDifference: { $subtract: [new Date(), '$lastReport'] }
                    }
                },
                {  
                    $match: {
                        $or: [ 
                            { substitute: { $ne: null } },
                            { reportDifference: { $gte: SYNCHRONIZING_ROUND * PERIOD } }
                        ] 
                    } 
                }
            ], { transactionSession }).toArray();
        }, transactionOptions);

        return nonworkingWorkers;
    }

    /** 
     * Computes balance factor.
     */
     computeBalanceFactor(nonworkingWorkers) {
        /* Compute how many substitutions this worker currently owns */
        const countOfCurrentlySubstituted = nonworkingWorkers.filter(worker => worker.substitute === this.workerId).length;
        /* Compute count of working workers */
        const countOfWorkingWorkers = this.maxWorkers - nonworkingWorkers.length;
        /* Compute remainder */
        const remainder = nonworkingWorkers.length % countOfWorkingWorkers === 0 ? 0 : 1;
        /* Return balance factor for this worker */
        return Math.floor(nonworkingWorkers.length / countOfWorkingWorkers) + remainder - countOfCurrentlySubstituted;
    }

    /** 
     * Check dead workers wtihout any substitution and try to substitute them.
     */
    async resolveDeadWorkers(transactionSession) {
        log.verbose(`SenderWorker:${this.workerId}`, 'Resolving dead workers...');
        const nonworkingWorkers = await this.getNonWorkingWorkers(transactionSession);
        /* Max amount of substitutions which this worker can do */
        let balanceFactor = this.computeBalanceFactor(nonworkingWorkers);

        /* Filter only non-working unsubstituted workers */
        const unsubstitutedWorkers = nonworkingWorkers.filter(worker => worker.substitute === null);
        /* Try to substitute at most ${balanceFactor} unsubstituted dead workers by this worker */
        for (const unsubstitutedWorker of unsubstitutedWorkers) {
            if (balanceFactor <= 0) {
                break;
            }

            /* Try to substitute unsubstitutedWorker in this transaction */
            await transactionSession.withTransaction(async () => {
                const workerStillUnsubstituted = await this.mongodb.collection('sender_workers')
                    .findOne({ _id: unsubstitutedWorker._id, substitute: null }, { transactionSession });

                if (!workerStillUnsubstituted) {
                    log.error(`SenderWorker:${this.workerId}`, `Dead worker ${unsubstitutedWorker._id} is already substituted!`);
                    await transactionSession.abortTransaction();
                }

                const updateResult = await this.mongodb.collection('sender_workers')
                    .updateOne({ _id: unsubstitutedWorker._id }, { $set: { state: SenderWorkerState.DEAD, substitute: this.workerId } }, { transactionSession });

                if (updateResult.modifiedCount) {
                    await this.mongodb.collection('sender_workers')
                        .updateMany({ substitute: unsubstitutedWorker._id }, { $set: { substitute: null } }, { transactionSession });
                    balanceFactor--;
                    log.info(`SenderWorker:${this.workerId}`, `Substituted: ${unsubstitutedWorker._id}  balanceFactor: ${balanceFactor}`);
                } else {
                    log.error(`SenderWorker:${this.workerId}`, `Missed DEAD worker ${unsubstitutedWorker._id} !`);
                    await transactionSession.abortTransaction();
                }

                /* If the transaction has not been aborted and worker is substituted by this worker, then push range of substituted worker */
                this.ranges.push(unsubstitutedWorker.range);
            }, transactionOptions);
        }
    }

    /** 
     * Check all invariants validity among all workers during the entire run of program and log errors.
     */
    async checkStateConsistency(transactionSession) {
    }

        
    /** 
     * After each worker iteration, check whether there are some SYNCHRONIZING workers waiting for their range that you
     * currently own and give it back to them.
     */
    async checkSynchronizingWorkers(transactionSession) {
        await transactionSession.withTransaction(async () => {
            const synchronizingWorkers = await this.mongodb.collection('sender_workers').find({ 
                state: SenderWorkerState.SYNCHRONIZING,
                substitute: this.workerId
            }, { transactionSession }).toArray();
    
            for (const synchronizingWorker of synchronizingWorkers) {
                await this.mongodb.collection('sender_workers')
                    .updateOne({ _id: synchronizingWorker._id }, { $set: { substitute: null } }, { transactionSession });
                
                /* Remove substituted range that belongs to synchronizingWorker */
                this.ranges = this.ranges.filter(range => range.from !== synchronizingWorker.range.from || range.to !== synchronizingWorker.range.to);
                log.info(`SenderWorker:${this.workerId}`, `Synchronizing worker ${synchronizingWorker._id} and ranges changed ${this.ranges}`);
            }
        }, transactionOptions);
    }

    /** 
     * Check balance factor for this worker and release some substituted workers (possibly still DEAD) to keep substitutions balanced.
     */
     async releaseRedundantSubstitutions(transactionSession) {
        // log.verbose(`SenderWorker:${this.workerId}`, 'Releasing redundant substitutions..');
        const nonworkingWorkers = await this.getNonWorkingWorkers(transactionSession);
        /* Compute current balance factor for this worker and release some substituted workers if it is negative value */
        const balanceFactor = this.computeBalanceFactor(nonworkingWorkers);
        if (balanceFactor >= 0) {
            return;
        } else {
            balanceFactor = Math.abs(balanceFactor);
        }

        /* Filter only worker ids substituted by this worker */
        const releasingWorkerIds = nonworkingWorkers.filter(worker => worker.substitute === this.workerId).slice(0, balanceFactor).map(worker => worker._id);
        log.verbose(`SenderWorker:${this.workerId}`, `Releasing redundant substitutions: ${JSON.stringify(releasingWorkerIds, null, 4)}`);
        /* Unsubstitute all chosen workers to keep substitutions balanced. */
        await transactionSession.withTransaction(async () => {
            await this.mongodb.collection('sender_workers').updateMany({ _id: { $in: releasingWorkerIds } }, { substitute: null }, { transactionSession });
        }, transactionOptions);
    }
}

module.exports = WorkerSynchronizer;
