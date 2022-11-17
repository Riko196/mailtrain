'use strict';

const crypto = require('crypto');

module.exports = {
    enforce,
    cleanupFromPost,
    filterObject,
    castToInteger,
    normalizeEmail,
    hashEmail,
    hashToUint32,
    sleep,
    getRandomFromRange
};

function enforce(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

function cleanupFromPost(value) {
    return (value || '').toString().trim();
}

function filterObject(obj, allowedKeys) {
    const result = {};
    for (const key in obj) {
        if (allowedKeys.has(key)) {
            result[key] = obj[key];
        }
    }

    return result;
}

function castToInteger(id, msg) {
    const val = parseInt(id);

    if (!Number.isInteger(val)) {
        throw new Error(msg || 'Invalid id');
    }

    return val;
}

function normalizeEmail(email) {
    const emailParts = email.split(/@/);

    if (emailParts.length !== 2) {
        return email;
    }

    const username = emailParts[0];
    const domain = emailParts[1].toLowerCase();

    return username + '@' + domain;
}

function hashEmail(email) {
    return crypto.createHash('sha512').update(normalizeEmail(email)).digest('base64');
}

function hashToUint32(hash) {
    let buffer = Buffer.from(hash, 'base64');
    let result = 0;
    let i = 0;

    for (const byte of buffer) {
        result += (byte << (8 * (4 - i - 1))) >>> 0;
        i++;
        if (i > 3) {
            break;
        }
    }

    return result;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getRandomFromRange(from, to) {
    return Math.floor(Math.random() * (to - from) + from);
}
