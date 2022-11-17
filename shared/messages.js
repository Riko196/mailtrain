'use strict'

const MessageType = {
    REGULAR: 0,
    TRIGGERED: 1,
    TEST: 2,
    SUBSCRIPTION: 3,
    API_TRANSACTIONAL: 4
};

const MessageStatus = {
    MIN: 0,

    SENT: 1,
    UNSUBSCRIBED: 2,
    BOUNCED: 3,
    COMPLAINED: 4,
    SCHEDULED: 5,

    FAILED: 6,

    MAX: 6
};

const MessageErrorType = {
    TRANSIENT: 0,
    PERMANENT: 1
};


module.exports = {
    MessageType,
    MessageStatus,
    MessageErrorType
};