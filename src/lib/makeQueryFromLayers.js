const R = require('ramda');
const knex = require('../utils/knex');
const makeTableName = require('./makeTableName');

const makeJoinQuery = require('./queries/layers/makeJoinQuery');

const { MAX_IN_MEMORY_ROWS } = require('../constants/boardDataMetaConstants');

const makeQueryFromLayers = (datasetId, { columns, layers, layerToggles }) =>
  R.pipe(makeJoinQuery(layers.joins), knex =>
    knex.limit(MAX_IN_MEMORY_ROWS).toString(),
  )(knex(makeTableName(datasetId)));

module.exports = makeQueryFromLayers;
