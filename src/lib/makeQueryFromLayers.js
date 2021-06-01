const R = require('ramda');
const { format } = require('sql-formatter');
const knex = require('../utils/knex');
const makeTableName = require('./makeTableName');

const makeJoinQuery = require('./queries/layers/makeJoinQuery');
const makeFilterQuery = require('./queries/layers/makeFilterQuery');
const makeGroupingQuery = require('./queries/layers/makeGroupingQuery');
const makeSortingQuery = require('./queries/layers/makeSortingQuery');
const { makeColumnSummaryQuery } = require('./queries/makeColumnSummaryQuery');

const { MAX_IN_MEMORY_ROWS } = require('../constants/boardDataMetaConstants');

const makeQueryFromLayers = (
  applyGrouping,
  applyIfVisible,
  datasetId,
  { columns, layers },
  { makeSummary = false } = {},
) =>
  R.pipe(
    applyIfVisible('joins', makeJoinQuery(datasetId, layers.joins)),
    applyIfVisible('filters', makeFilterQuery(layers.filters)),
    currentKnex => knex.with('query', currentKnex).select().from('query'),
    applyIfVisible(
      'groupings',
      makeGroupingQuery(datasetId, layers.groupings, columns),
    ),
    knex =>
      makeSummary
        ? knex
        : applyIfVisible(
            'sortings',
            makeSortingQuery(applyGrouping, layers.sortings, layers.groupings),
          )(knex),
    knex => (makeSummary ? makeColumnSummaryQuery(columns, knex) : knex),
    knex =>
      makeSummary ? knex.toString() : knex.limit(MAX_IN_MEMORY_ROWS).toString(),
    x => {
      console.log('current query\n---\n', format(x.toString()), '\n---');
      return x;
    },
  )(knex(makeTableName(datasetId)));

module.exports = makeQueryFromLayers;
