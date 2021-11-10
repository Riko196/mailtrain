'use strict';
const { getAdminUser, getFakeUser } = require('../data-generator');

exports.seed = (knex, Promise) => (async() => {
  // Generates new random users
  const countOfUsers = 50;
  await knex('users').update(getAdminUser()).where('id', 1);
  for (let i = 2; i <= countOfUsers; i++) {
    const fakeUser = getFakeUser();
    const user = await knex('users').select('*').where("id", i).first();
    if (user) {
      await knex('users').update(fakeUser).where('id', user.id);
    } else {
      await knex('users').insert(fakeUser);
    }
  }
})();
