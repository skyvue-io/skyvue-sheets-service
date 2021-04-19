const R = require('ramda');
const knex = require('../../utils/knex');

const groupLayer = {
  groupedBy: ['d0b12ecc-5385-4681-9543-70e7b15dcb0d'],
  columnAggregates: {
    'c491b7de-61a0-4b0e-abd0-e12f08eb36f8': 'count',
  },
};

/*
select 'd0b12ecc-5385-4681-9543-70e7b15dcb0d', count("c491b7de-61a0-4b0e-abd0-e12f08eb36f8") from tableName
group by 'd0b12ecc-5385-4681-9543-70e7b15dcb0d'
*/

const mapAggregatesToPostgres = (colId_, aggregation, colLookup) => {
  const colId = `"${colId_}"`;
  const colValue = `"${colLookup[colId_].value}"`;
  return {
    sum: `avg(${colId}) as ${colValue}`,
    mean: `avg(${colId}) as ${colValue}`,
    median: '', // later
    countDistinct: `count(distinct ${colId}) as ${colValue}`,
    count: `count(${colId}) as ${colValue}`,
    max: `max(${colId}) as ${colValue}`,
    min: `min(${colId}) as ${colValue}`,
    stdev: `stddev(${colId}) as ${colValue}`,
  }[aggregation];
};

const buildAggregateLayersQuery = R.curry((tableName, boardData) => {
  const { layers, layerToggles } = boardData;
  const { groupings, sortings } = layers;
  const { groupedBy, columnAggregates } = groupings ?? {};

  const colLookup = R.indexBy(R.prop('_id'))(boardData.columns);

  const selectStatementParams = [
    ...(groupedBy?.length > 0 && layerToggles.groupings
      ? [
          ...groupedBy,
          ...Object.keys(columnAggregates).map(key =>
            knex.raw(mapAggregatesToPostgres(key, columnAggregates[key], colLookup)),
          ),
        ]
      : []),
  ];

  const attachGroupingToQuery = query => {
    if (!layerToggles.groupings || !groupedBy?.length) return query.clone();
    groupedBy.forEach(col => query.groupBy(col));
    return query.clone();
  };

  const attachSortingToQuery = query => {
    if (!sortings?.length) return query.clone();
    sortings
      .filter(sorting => groupedBy.includes(sorting.key))
      .forEach(({ key, direction }) => query.orderBy(key, direction));
    return query.clone();
  };

  return R.pipe(attachGroupingToQuery, attachSortingToQuery, query =>
    query.toString(),
  )(knex(tableName).select(...selectStatementParams));
});

//   applyColSummaries, // async?
//   applyGrouping, CHECK
//   applySorting, CHECK
//   applyRowIndeces,

module.exports = buildAggregateLayersQuery;
