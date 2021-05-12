const R = require('ramda');

const makeSortingQuery = R.curry(
  (applyGrouping, sortingLayer, groupingLayer, knex_) => {
    const knex = knex_.clone();
    const { groupedBy, columnAggregates } = groupingLayer ?? {};

    sortingLayer
      .filter(sorting =>
        applyGrouping
          ? groupedBy.includes(sorting.key) ||
            Object.keys(columnAggregates).includes(sorting.key)
          : true,
      )
      .forEach(({ key, direction }) => knex.orderBy(key, direction));

    return knex;
  },
);

module.exports = makeSortingQuery;
