'use strict';

const { getMongoDB, getNewTransactionSession, transactionOptions } = require('../../mongodb');
const { SenderWorkerState } = require('./init');
const log = require('../../log');

/** It defines period in which workers report alive state or synchronize with each other. */
const PERIOD = 5 * 1000;
/** It defines round of period in which workers synchronize with each other. */
const SYNCHRONIZING_ROUND = 5;

/**
 * SenderWorker component which periodically synchronize worker to which it belongs with all other workers.
 */
class WorkerSynchronizer {
    constructor(maxWorkers, workerId, state, ranges) {
        /* Get number of maximum workers who can be spawned */
        this.maxWorkers = maxWorkers;
        /* Get WorkerID */
        this.workerId = workerId;
        /* Get the current worker state (used as a reference object) */
        this.state = state;
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
        const mongoDBSession = getNewTransactionSession();
        try {
            await this.reportAliveState();
            if (this.state.value === SenderWorkerState.WORKING && this.synchronizingNumber === SYNCHRONIZING_ROUND) {
                log.verbose(`SenderWorker:${this.workerId}`, 'SYNCHRONIZING ROUND...');
                await this.resolveDeadWorkers(mongoDBSession);
            } 
        } catch(error){
            log.error(`SenderWorker:${this.workerId}`, `Unexpected error occurred during synchronization: ${error}`);
            log.error(`SenderWorker:${this.workerId}`, error.stack);
        } finally {
            log.info(`SenderWorker:${this.workerId}`, `RANGES: ${JSON.stringify(this.ranges, null, 4)}`);
            this.synchronizingNumber = this.synchronizingNumber === SYNCHRONIZING_ROUND ? 0 : this.synchronizingNumber + 1;    
            await mongoDBSession.endSession();
            setTimeout(this.synchronizerPeriodicCheck.bind(this), PERIOD);
        }
    }

    /** 
     * Write to the MongoDB collection that this worker is still alive.
     */
    async reportAliveState() {
        log.verbose(`SenderWorker:${this.workerId}`, 'Periodic report...');
        /* No need for transaction */
        await this.mongodb.collection('sender_workers').updateOne(
            { _id: this.workerId },
            { $set: { lastReport: new Date() } }
        );    
    }

    /** 
     * Get all non-working workers (all substituted workers (SYNCHRONIZING or DEAD state)) or
     * non-working but not yet substituted (lastReport is too old (WORKING or DEAD state)). 
     * 
     * @argument mongoDBSession - MongoDB transaction session
     * @returns list of non-working workers.
     */
    async getNonWorkingWorkers(mongoDBSession) {
        let nonworkingWorkers = [];
        await mongoDBSession.withTransaction(async () => {
            nonworkingWorkers = await this.mongodb.collection('sender_workers').aggregate([{
                    $addFields: {
                        reportDifference: { $subtract: [new Date(), '$lastReport'] }
                    }
                }, {  
                    $match: {
                        $or: [ 
                            { substitute: { $ne: null } },
                            { reportDifference: { $gte: SYNCHRONIZING_ROUND * PERIOD } }
                        ] 
                    } 
                }
            ], { session: mongoDBSession }).toArray();
        }, transactionOptions);

        return nonworkingWorkers;
    }

    /** 
     * Computes balance factor for list of non-working SenderWorkers.
     * 
     * @argument nonworkingWorkers - list of the current non-working SenderWorkers
     * @returns balance factor for the nonworkingWorkers list.
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
     * 
     * @argument mongoDBSession - MongoDB transaction session
     */
    async resolveDeadWorkers(mongoDBSession) {
        log.verbose(`SenderWorker:${this.workerId}`, 'Resolving dead workers...');
        const nonworkingWorkers = await this.getNonWorkingWorkers(mongoDBSession);
        // log.info(`SenderWorker:${this.workerId}`, `Nonworking: ${nonworkingWorkers.length} ${JSON.stringify(nonworkingWorkers, null, 4)}`);
        /* If everyone is working, there is no substitute and no need to release someone */
        if (!nonworkingWorkers.length) {
            return;
        }
        /* Max amount of substitutions which this worker can do */
        let balanceFactor = this.computeBalanceFactor(nonworkingWorkers);

        /* Filter only non-working unsubstituted workers */
        const unsubstitutedWorkers = nonworkingWorkers.filter(worker => worker.substitute === null);
        /* Try to substitute at most ${balanceFactor} unsubstituted dead workers by this worker */
        for (const unsubstitutedWorker of unsubstitutedWorkers) {
            if (balanceFactor <= 0) {
                break;
            }

            try {
                /* Try to substitute unsubstitutedWorker in this transaction */
                await mongoDBSession.withTransaction(async () => {
                    const workerStillUnsubstituted = await this.mongodb.collection('sender_workers').findOne(
                            { _id: unsubstitutedWorker._id, lastReport: unsubstitutedWorker.lastReport, substitute: null },
                            { session: mongoDBSession }
                        );
                    
                    if (!workerStillUnsubstituted) {
                        throw new Error('Transaction aborted!');
                    }
                    
                    log.info(`SenderWorker:${this.workerId}`, `Worker still unsub: ${unsubstitutedWorker._id} ${JSON.stringify(workerStillUnsubstituted, null, 4)}`);

                    /* If a unsubstitutedWorker is still unsubstituted, then substitute him by this worker */
                    await this.mongodb.collection('sender_workers').updateOne(
                        { _id: unsubstitutedWorker._id },
                        { $set: { state: SenderWorkerState.DEAD, substitute: this.workerId } },
                        { session: mongoDBSession }
                    );

                    /* Unsubstitute all his substitutions */
                    await this.mongodb.collection('sender_workers').updateMany(
                        { substitute: unsubstitutedWorker._id },
                        { $set: { substitute: null } },
                        { session: mongoDBSession }
                    );
                }, transactionOptions);

                /* If the transaction has not been aborted and worker is substituted by this worker, then push range of substituted worker and decrease balance factor */
                this.ranges.push(unsubstitutedWorker.range);
                balanceFactor--;
            } catch(error) {
                log.error(`SenderWorker:${this.workerId}`, `Dead worker ${unsubstitutedWorker._id} is already substituted!`);
                log.error(error);
            }
        }
    }
        
    /** 
     * After each worker iteration, check whether there are some SYNCHRONIZING workers waiting for their range that you
     * currently own and release them.
     * 
     * @argument mongoDBSession - MongoDB transaction session
     */
    async releaseSynchronizingWorkers(mongoDBSession) {
        try {
            let synchronizingWorkers = [];
            await mongoDBSession.withTransaction(async () => {
                /* Get all synchronizing workers waiting for release */
                synchronizingWorkers = await this.mongodb.collection('sender_workers').find({ 
                    state: SenderWorkerState.SYNCHRONIZING,
                    substitute: this.workerId
                }, { session: mongoDBSession }).toArray();
                
                /* Release all synchronizing workers */
                const updateResult = await this.mongodb.collection('sender_workers').updateMany(
                    { _id: { $in: synchronizingWorkers.map(worker => worker._id) } },
                    { $set: { substitute: null } },
                    { session: mongoDBSession }
                );

                /* Abort transaction if not all synchronizing workers have been updated */
                if (updateResult.modifiedCount != synchronizingWorkers.length) {
                    throw new Error('Transaction aborted!');
                }
            }, transactionOptions);

            /* Remove ranges that belong to synchronizing workers */
            for (const synchronizingWorker of synchronizingWorkers) {
                this.ranges = this.ranges.filter(range => range.from !== synchronizingWorker.range.from || range.to !== synchronizingWorker.range.to);
            }
        } catch(error) {
            log.error(`SenderWorker:${this.workerId}`, 'Releasing synchronizing workers transaction aborted!');
            log.error(error);
        }
    }

    /** 
     * Check balance factor for this worker and release some substituted workers (possibly still DEAD) to keep substitutions balanced.
     * 
     * @argument mongoDBSession - MongoDB transaction session
     */
    async releaseRedundantSubstitutions(mongoDBSession) {
        // log.verbose(`SenderWorker:${this.workerId}`, 'Releasing redundant substitutions..');
        const nonworkingWorkers = await this.getNonWorkingWorkers(mongoDBSession);
        /* If everyone is working, there is no substitute and no need to release someone */
        if (!nonworkingWorkers.length) {
            return;
        }
        /* Compute current balance factor for this worker and release some substituted workers if it is negative value */
        let balanceFactor = this.computeBalanceFactor(nonworkingWorkers);
        if (balanceFactor >= 0) {
            return;
        } else {
            balanceFactor = Math.abs(balanceFactor);
        }

        /* Filter only worker ids substituted by this worker */
        const releasingWorkers = nonworkingWorkers
            .filter(worker => worker.substitute === this.workerId).slice(0, balanceFactor);
        log.verbose(`SenderWorker:${this.workerId}`, `Releasing redundant substitutions: ${JSON.stringify(releasingWorkers, null, 4)}`);

        /* Unsubstitute all chosen workers to keep substitutions balanced. */
        try {
            await mongoDBSession.withTransaction(async () => {
                const updateResult = await this.mongodb.collection('sender_workers').updateMany(
                    { _id: { $in: releasingWorkers.map(worker => worker._id) } },
                    { $set: { substitute: null } },
                    { session: mongoDBSession }
                );
    
                /* Abort transaction if no all releasing workers have been updated */
                if (updateResult.modifiedCount != releasingWorkers.length) {
                    throw new Error('Transaction aborted!');
                }
            }, transactionOptions);

            /* Remove ranges that belong to releasing workers */
            for (const releasingWorker of releasingWorkers) {
                this.ranges = this.ranges.filter(range => range.from !== releasingWorker.range.from || range.to !== releasingWorker.range.to);
            }
        } catch(error) {
            log.error(`SenderWorker:${this.workerId}`, 'Releasing redundant workers transaction aborted!');
            log.error(error);
        }
    }

    /** 
     * Check whether there is a potential deadlock and solve it if so.
     */
    async solvePotentialDeadlock() {
        const mongoDBSession = getNewTransactionSession();
        try {
            let aliveWorkers = [];
            await mongoDBSession.withTransaction(async () => {
                aliveWorkers = await this.mongodb.collection('sender_workers').aggregate([
                    { $addFields: { reportDifference: { $subtract: [new Date(), '$lastReport'] } } },
                    { $match: { _id: { $ne: this.workerId }, state: SenderWorkerState.WORKING, reportDifference: { $lt: SYNCHRONIZING_ROUND * PERIOD } } }
                ], { session: mongoDBSession }).toArray();
        
                if (!aliveWorkers.length) {
                    /* Set all WORKING workers to DEAD state */
                    await this.mongodb.collection('sender_workers').updateMany(
                        { state: SenderWorkerState.WORKING },
                        { $set: { state: SenderWorkerState.DEAD } },
                        { session: mongoDBSession }
                    );
                    
                    /* Remove all substitutions */
                    await this.mongodb.collection('sender_workers').updateMany(
                        { substitute: { $ne: null } },
                        { $set: { substitute: null } },
                        { session: mongoDBSession }
                    );
                    
                    /* Set yourself to WORKING state */
                    await this.mongodb.collection('sender_workers').updateOne(
                        { _id: this.workerId },
                        { $set: { state: SenderWorkerState.WORKING, lastReport: new Date(), substitute: null } },
                        { session: mongoDBSession }
                    );
                    log.info(`SenderWorker:${this.workerId}`, `Deadlock detected! Solving problem...`);
                }
            }, transactionOptions);
        } catch(error) {
            log.error(`SenderWorker:${this.workerId}`, `Unexpected error detected during resolving deadlock ${error}`);
            log.error(error);
        } finally {
            await mongoDBSession.endSession();
        }
    } 
}

module.exports = WorkerSynchronizer;
