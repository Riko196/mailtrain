exports.up = (knex, Promise) => (async() => {
    await knex.schema.raw('ALTER TABLE `queued` ADD COLUMN `status` tinyint(4) unsigned NOT NULL DEFAULT 0');
})();

exports.down = (knex, Promise) => (async() => {
    await knex.schema.table('queued', table => {
        table.dropColumn('status');
    });
})();
