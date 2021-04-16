const R = require('ramda');

const loadS3ToPostgres = require('./loadS3ToPostgres');
const makePostgres = require('./postgres');
const knex = require('../utils/knex');
const pgQueryToBoardData = require('../lib/pgQueryToBoardData');

const makeJoinQuery = (datasetId, joinLayer) =>
  knex.select().table(`${datasetId}_base`).toString();

// plucks data from s3
// ensures that joined tables are in postgres prior to returning joined response
const queryJoinedDataset = async (datasetId, baseState) => {
  const postgres = await makePostgres();
  if (
    !baseState?.layerToggles?.joins ||
    !baseState?.joinLayer ||
    Object.keys(baseState?.joinLayer ?? {}).length === 0
  ) {
    const res = await postgres.query(
      knex.select().table(`${datasetId}_base`).toString(),
    );

    return pgQueryToBoardData(res, baseState);
  }

  await loadS3ToPostgres(datasetId);
  return postgres.query(makeJoinQuery(datasetId, joinLayer));
};

module.exports = queryJoinedDataset;
