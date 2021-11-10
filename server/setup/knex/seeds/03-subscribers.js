'use strict';
const { getFakeUniqueSubscriber } = require('../data-generator');

exports.seed = (knex, Promise) => (async() => {
  // For each list generates new random subscribers
  const countOfLists = (await knex('lists').select('id')).length;
  const countOfSubcribers = 5000;
  let uniqueID = 1;
  for (let i = 1; i <= countOfLists; i++) {
    let list = await knex('lists').select('*').where('id', i).first()
    list.subscribers = countOfSubcribers
    await knex('lists').update(list).where('id', list.id);

    const tableExists = await knex.schema.hasTable('subscription__' + i);
    if (!tableExists) {
        await knex.schema.createTable('subscription__' + i, table => {
            table.increments('id').primary();
            table.string('cid');
            table.string('email');
            table.string('hash_email');
            table.integer('source_email', 11);
            table.string('opt_in_ip', 100);
            table.string('opt_in_country', 2);
            table.string('tz', 100);
            table.integer('status').unsigned().defaultTo(1);
            table.integer('is_test').unsigned().defaultTo(0);
            table.timestamp('status_change');
            table.timestamp('unsubscribed');
            table.timestamp('latest_open ');
            table.timestamp('latest_click');
            table.timestamp('created').defaultTo(knex.fn.now());
            table.timestamp('updated').defaultTo(knex.fn.now());
        });
    }

    for (let j = 1; j <= countOfSubcribers; j++, uniqueID++) {
        const fakeUniqueSubscriber = getFakeUniqueSubscriber(uniqueID);
        const subscriber = await knex('subscription__' + i).select('*').where('id', j).first();
        if (subscriber) {
            await knex('subscription__' + i).update(fakeUniqueSubscriber).where('id', subscriber.id);
        } else {
            await knex('subscription__' + i).insert(fakeUniqueSubscriber);
        }
    }
  }
})();
