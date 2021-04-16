const R = require('ramda');
const knex = require('../../utils/knex');
const mapRowsToInsertData = require('../mapRowsToInsertData');

const makeWorkingTableInsertQuery = R.curry((datasetId, baseState) =>
  knex(`${datasetId}_working`)
    .insert(mapRowsToInsertData(baseState.columns)(baseState.rows))
    .onConflict('_id')
    .merge()
    .returning('*')
    .toString(),
);

module.exports = makeWorkingTableInsertQuery;
