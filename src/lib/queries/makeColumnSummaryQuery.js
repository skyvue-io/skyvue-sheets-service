const R = require('ramda');

const makeColumnSummaryQuery = R.curry((columns, knex_) => {
  const knex = knex_.clone();

  return knex.select('foo', 'bar');
});

module.exports = makeColumnSummaryQuery;
