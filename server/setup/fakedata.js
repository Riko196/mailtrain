'use strict';

let { getFakeSubscribers } = require('./knex/data-generator');

process.stdout.write('First name,Last name,E-Mail,Company,Phone number');
getFakeSubscribers();
