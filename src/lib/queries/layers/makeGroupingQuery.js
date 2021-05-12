const R = require('ramda');
const knex = require('../../../utils/knex');

const makeRaw = input => knex.raw(input);

const mapAggregatesToSQL = (colId_, aggregation, colLookup) => {
  const colId = `"${colId_}"`;
  const colValue = colLookup[colId_].value;
  return {
    sum: `sum(${colId}) as ${colId}`,
    mean: `avg(${colId}) as ${colId}`,
    median: '', // later
    countDistinct: `count(distinct ${colId}) as ${colId}`,
    count: `count(${colId}) as ${colId}`,
    max: `max(${colId}) as ${colId}`,
    min: `min(${colId}) as ${colId}`,
    stdev: `stddev(${colId}) as ${colId}`,
  }[aggregation];
};

const makeGroupingQuery = R.curry((datasetId, groupingLayer, columns, knex_) => {
  const knex = knex_.clone();

  const { groupedBy, columnAggregates } = groupingLayer ?? {};

  const colLookup = R.indexBy(R.prop('_id'))(columns);
  const selectStatementParams = [
    ...groupedBy,
    ...Object.keys(columnAggregates).map(key =>
      makeRaw(mapAggregatesToSQL(key, columnAggregates[key], colLookup)),
    ),
  ];

  knex.select(...selectStatementParams);
  groupedBy.forEach(col => knex.groupBy(col));

  return knex.clone();
});

module.exports = makeGroupingQuery;
