const appBuilder = require('../app-builder');
const config = require('./config');
const log = require('./log');
const http = require('http');
const bluebird = require('bluebird');

const host = config.www.host;

async function startHTTPServer(appType, appName, port) {
    const app = await appBuilder.createApp(appType, port);
    app.set('port', port);

    const server = http.createServer(app);

    server.on('error', err => {
        if (err.syscall !== 'listen') {
            throw err;
        }

        const bind = typeof port === 'string' ? 'Pipe ' + port : 'Port ' + port;

        // handle specific listen errors with friendly messages
        switch (err.code) {
            case 'EACCES':
                log.error('Express', '%s requires elevated privileges', bind);
                return process.exit(1);
            case 'EADDRINUSE':
                log.error('Express', '%s is already in use', bind);
                return process.exit(1);
            default:
                throw err;
        }
    });

    server.on('listening', () => {
        const addr = server.address();
        const bind = typeof addr === 'string' ? 'pipe ' + addr : 'port ' + addr.port;
        log.info('Express', 'WWW server [%s] listening on %s', appName, bind);
    });

    const serverListenAsync = bluebird.promisify(server.listen.bind(server));
    await serverListenAsync({ port, host });
}

module.exports.startHTTPServer = startHTTPServer;
