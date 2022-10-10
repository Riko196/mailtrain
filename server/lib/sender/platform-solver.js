'use strict';

const config = require('../config');
const { enforce } = require("../helpers");

/** It defines all supported platforms under which worker is running. */
const PlatformType = {
    CENTRALIZED: 0,
    SLURM: 1
};

/**
 * The class with static methods that is used by SenderWorker to obtain the necessary variables depending 
 * on the platform on which it runs (Centralized, SLURM, Kubernetes, ...).
 */
class PlatformSolver {
    static runningPlatform;
    
    static findPlatform() {
        if (process.env.WORKER_ID) {
            /* If it is running centralized */
            this.runningPlatform = PlatformType.CENTRALIZED;
        } else if (process.env.SLURM_PROCID) {
            /* If it is running upon SLURM */
            this.runningPlatform = PlatformType.SLURM;
        } else {
            enforce(false, 'Unsupported SenderWorker platform!');
        }
    }

    /**
     * According to running platform return variable which defines worker ID.
     */
    static getWorkerId() {
        if (this.runningPlatform === PlatformType.CENTRALIZED) {
            return Number.parseInt(process.env.WORKER_ID);
        } else {
            return Number.parseInt(process.env.SLURM_PROCID);
        }
    }

    /**
     * According to running platform return amount of workers.
     */
    static getNumberOfWorkers() {
        if (this.runningPlatform === PlatformType.CENTRALIZED) {
            return config.sender.workers;
        } else {
            return process.env.SLURM_NTASKS
        }
    }

    /**
     * Return whether worker is running in Centralized mode.
     */
    static isCentralized() {
        return this.runningPlatform === PlatformType.CENTRALIZED;
    }
}

PlatformSolver.findPlatform();

module.exports = PlatformSolver;
