const R = require('ramda');
const knex = require('../../utils/knex');

const makePredicateMap = colId_ => {
  const colId = `"${colId_}"`;
  return {
    uniqueValues: {
      predicate: 'uniqueValues',
      sql: `count(distinct ${colId})`,
      types: ['number', 'string'],
    },
    sum: {
      predicate: 'sum',
      sql: `sum(${colId})`,
      types: ['number'],
    },
    mean: {
      predicate: 'mean',
      sql: `avg(${colId})`,
      types: ['number'],
    },
    min: {
      predicate: 'min',
      sql: `min(${colId})`,
      types: ['number'],
    },
    max: {
      predicate: 'max',
      sql: `max(${colId})`,
      types: ['number'],
    },
  };
};

const makeColumnSummaryQuery = R.curry((columns, query_) => {
  const query = query_.clone();

  columns.forEach(col => {
    const predicateMap = makePredicateMap(col._id);

    const valuesToApply =
      Object.values(predicateMap).filter(pred =>
        pred.types.includes(col.dataType),
      ) ?? [];

    valuesToApply.forEach(value => {
      query.select(knex.raw(`${value.sql} as "${value.predicate}-${col._id}"`));
    });
  });

  return query;
});

module.exports = { makeColumnSummaryQuery, makePredicateMap };
