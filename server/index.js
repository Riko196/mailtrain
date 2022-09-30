'use strict';

const config = require('./lib/config');
const log = require('./lib/log');
const appBuilder = require('./app-builder');
const { startHTTPServer } = require('./lib/http-server');
const triggers = require('./services/triggers');
const gdprCleanup = require('./services/gdpr-cleanup');
const importer = require('./lib/importer');
const feedcheck = require('./lib/feedcheck');
const verpServer = require('./services/verp-server');
const testServer = require('./services/test-server');
const postfixBounceServer = require('./services/postfix-bounce-server');
const tzupdate = require('./services/tzupdate');
const { dbcheck } = require('./lib/dbcheck');
const sender = require('./lib/sender/sender');
const hapublic = require('./lib/hapublic/hapublic');
const reportProcessor = require('./lib/report-processor');
const executor = require('./lib/executor');
const privilegeHelpers = require('./lib/privilege-helpers');
const knex = require('./lib/knex');
const mongodb = require('./lib/mongodb');
const shares = require('./models/shares');
const { AppType } = require('../shared/app');
const builtinZoneMta = require('./lib/builtin-zone-mta');
const klawSync = require('klaw-sync');

const { uploadedFilesDir } = require('./lib/file-helpers');
const { reportFilesDir } = require('./lib/report-helpers');
const { filesDir } = require('./models/files');

const trustedPort = config.www.trustedPort;
const sandboxPort = config.www.sandboxPort;
const publicPort = config.www.publicPort;

if (config.title) {
    process.title = config.title;
}

// ---------------------------------------------------------------------------------------
// Start the whole circus
// ---------------------------------------------------------------------------------------
async function init() {
    /* Check both databases MySQL and MongoDB whether they work and are correctly setup */
    await dbcheck();

    await knex.migrate.latest(); // And now the current migration with Knex

    log.info('MongoDB', 'Connecting to MongoDB cluster...');
    await mongodb.connectToMongoDB();
    log.info('MongoDB', 'Successfully connected to MongoDB cluster!');
    
    await shares.regenerateRoleNamesTable();
    await shares.rebuildPermissions();

    await privilegeHelpers.ensureMailtrainDir(filesDir);

    // Update owner of all files under 'files' dir. This should not be necessary, but when files are copied over,
    // the ownership needs to be fixed.
    for (const dirEnt of klawSync(filesDir, {})) {
        await privilegeHelpers.ensureMailtrainOwner(dirEnt.path);
    }

    await privilegeHelpers.ensureMailtrainDir(uploadedFilesDir);
    await privilegeHelpers.ensureMailtrainDir(reportFilesDir);

    await executor.spawn();
    await testServer.start();
    await verpServer.start();
    await builtinZoneMta.spawn();

    await startHTTPServer(AppType.TRUSTED, 'trusted', trustedPort);
    await startHTTPServer(AppType.SANDBOXED, 'sandbox', sandboxPort);
    await startHTTPServer(AppType.PUBLIC, 'public', publicPort);
    await hapublic.spawn();

    privilegeHelpers.dropRootPrivileges();

    tzupdate.start();

    await importer.spawn();
    await feedcheck.spawn();
    await sender.spawn();

    triggers.start();
    gdprCleanup.start();

    await postfixBounceServer.start();

    await reportProcessor.init();

    log.info('Service', 'All services started');
    appBuilder.setReady();
}

init().catch(err => {log.error('', err); process.exit(1); });


