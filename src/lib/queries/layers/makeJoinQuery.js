const R = require('ramda');

const makeJoinQuery = R.curry((joinLayers, knex) => knex.clone());

module.exports = makeJoinQuery;
