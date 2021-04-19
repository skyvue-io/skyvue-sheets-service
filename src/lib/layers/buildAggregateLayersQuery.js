const R = require('ramda');
const knex = require('../../utils/knex');

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

  const applyGrouping = groupedBy?.length > 0 && layerToggles.groupings;
  const selectStatementParams = [
    ...(applyGrouping
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
      .filter(sorting => (applyGrouping ? groupedBy.includes(sorting.key) : true))
      .forEach(({ key, direction }) => query.orderBy(key, direction));
    return query.clone();
  };

  return R.pipe(
    attachGroupingToQuery,
    attachSortingToQuery,
    query => query.limit(30000),
    query => query.toString(),
  )(knex(tableName).select(...selectStatementParams));
});

module.exports = buildAggregateLayersQuery;
