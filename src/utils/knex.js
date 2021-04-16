const knex = require('knex')({
  client: 'pg',
  version: '11.9',
});

module.exports = knex;
