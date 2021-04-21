const mapRowsToInsertData = require('../lib/mapRowsToInsertData');
const knex = require('../utils/knex');

const createTableFromColumns = require('../lib/queries/createTableFromColumns');

const s3 = require('./aws');
const makePostgres = require('./postgres');

const Bucket = 'skyvue-datasets-temp';

const loadS3ToPostgres = async datasetId => {
  const postgres = await makePostgres();
  const s3Params = {
    Bucket: 'skyvue-datasets-temp',
    Key: datasetId,
  };

  try {
    const columns = await s3
      .getObject({
        Bucket,
        Key: `${datasetId}/columns`,
      })
      .promise();

    const columnData = JSON.parse(columns.Body.toString('utf-8'));

    await postgres.query(
      createTableFromColumns(`${datasetId}_base`, columnData.columns),
    );

    const allRows = await s3
      .listObjects({
        Bucket,
        Prefix: `${datasetId}/rows/`,
      })
      .promise();

    allRows.Contents.forEach(async item => {
      const part = await s3
        .getObject({
          Bucket,
          Key: item.Key,
        })
        .promise();

      const data = JSON.parse(part.Body.toString('utf-8'));
      const insertQuery = knex(`${datasetId}_base`)
        .insert(mapRowsToInsertData(columnData.columns)(data))
        .onConflict('_id')
        .merge()
        .toString();

      await postgres.query(insertQuery);
    });

    return {
      success: true,
      baseState: columnData,
    };
  } catch (e) {
    console.log('error loading dataset from s3', s3Params, e);
    return {
      success: false,
    };
  }
};

module.exports = loadS3ToPostgres;
