const R = require('ramda');
const knex = require('../utils/knex');
const makeTableName = require('./makeTableName');

const makeJoinQuery = require('./queries/layers/makeJoinQuery');

const { MAX_IN_MEMORY_ROWS } = require('../constants/boardDataMetaConstants');

const makeQueryFromLayers = (
  applyIfVisible,
  datasetId,
  { columns, layers, layerToggles },
) =>
  R.pipe(
    applyIfVisible('joins', makeJoinQuery(datasetId, layers.joins)),
    x => {
      console.log(x.toString());
      return x;
    },
    knex => knex.limit(MAX_IN_MEMORY_ROWS).toString(),
  )(knex(makeTableName(datasetId)));

module.exports = makeQueryFromLayers;
