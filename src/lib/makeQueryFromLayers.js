const R = require('ramda');
const { format } = require('sql-formatter');
const knex = require('../utils/knex');
const makeTableName = require('./makeTableName');

const makeJoinQuery = require('./queries/layers/makeJoinQuery');
const makeFilterQuery = require('./queries/layers/makeFilterQuery');
const makeGroupingQuery = require('./queries/layers/makeGroupingQuery');
const makeSortingQuery = require('./queries/layers/makeSortingQuery');

const { MAX_IN_MEMORY_ROWS } = require('../constants/boardDataMetaConstants');

const makeQueryFromLayers = (
  applyGrouping,
  applyIfVisible,
  datasetId,
  { columns, layers },
) =>
  R.pipe(
    applyIfVisible('joins', makeJoinQuery(datasetId, layers.joins)),
    applyIfVisible('filters', makeFilterQuery(layers.filters)),
    currentKnex => knex.with('query', currentKnex).select().from('query'),
    applyIfVisible(
      'groupings',
      makeGroupingQuery(datasetId, layers.groupings, columns),
    ),
    applyIfVisible(
      'sortings',
      makeSortingQuery(applyGrouping, layers.sortings, layers.groupings),
    ),
    x => {
      console.log('current query', format(x.toString()));
      return x;
    },
    knex => knex.limit(MAX_IN_MEMORY_ROWS).toString(),
  )(knex(makeTableName(datasetId)));

module.exports = makeQueryFromLayers;
