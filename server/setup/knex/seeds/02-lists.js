'use strict';
const { getRandomList, getFakeSubscriber } = require('../data-generator');

exports.seed = (knex, Promise) => (async() => {
  // Generates new random lists
  const countOfLists = 50;
  for (let i = 1; i <= countOfLists; i++) {
    const randomList = getRandomList();
    const list = await knex('lists').select('*').where("id", i).first();
    if (list) {
      await knex('lists').update(randomList).where('id', list.id);
    } else {
      await knex('lists').insert(randomList);
    }
  }
})();
