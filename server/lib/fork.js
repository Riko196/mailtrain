'use strict';

const builtinFork = require('child_process').fork;

/* If this is Mailtrain root process */
if (process.title === 'node') {
    /* Catch Ctrl+C */
    process.on('SIGINT', () => {}); 
    /* Catch kill process */
    process.on('SIGTERM', () => {}); 
}

const children = [];

process.on('exit', function() {
    for (const child of children) {
        child.send('exit');
    }
});

function fork(path, args, opts) {
    const child = builtinFork(path, args, opts);

    children.push(child);
    return child;
}

module.exports.fork = fork;
