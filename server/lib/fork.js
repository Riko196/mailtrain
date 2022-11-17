'use strict';

const builtinFork = require('child_process').fork;

const children = [];

function safelyTurnOffMailtrain() {
    for (const child of children) {
        child.send('exit');
    }
}

function fork(path, args, opts) {
    const child = builtinFork(path, args, opts);
    children.push(child);
    return child;
}

/* If this is Mailtrain root process */
if (process.title === 'node') {
    /* Catch Ctrl+C */
    process.on('SIGINT', safelyTurnOffMailtrain); 
    /* Catch kill process */
    process.on('SIGTERM', safelyTurnOffMailtrain); 
}

module.exports.fork = fork;
