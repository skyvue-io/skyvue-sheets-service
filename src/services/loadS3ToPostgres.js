const R = require('ramda');
const knex = require('knex')({
  client: 'pg',
});

const s3 = require('./aws');
const makePostgres = require('./postgres');

const parseJson = require('../utils/parseJson');

const Bucket = 'skyvue-datasets';

const loadS3ToPostgres = async datasetId => {
  const postgres = await makePostgres();
  const s3Params = {
    Bucket: 'skyvue-datasets',
    Key: datasetId,
  };

  try {
    const columns = await s3
      .getObject({
        Bucket,
        Key: `${datasetId}/columns`,
      })
      .promise();
    const columnData = await parseJson(columns.Body.toString('utf-8'));

    const createTableQuery = knex.schema
      .createTableIfNotExists(datasetId, table => {
        table.string('rowId');
        columnData.columns.forEach(col => {
          table.string(col._id);
        });
      })
      .toString();

    console.log(createTableQuery);

    await postgres.query(createTableQuery);

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

      const insertData = R.pipe(
        R.map(row => ({
          ...row,
          cells: row.cells.map((cell, index) => ({
            ...cell,
            columnId: columnData.columns[index]?._id,
          })),
        })),
        R.map(row => [
          { rowId: row._id },
          ...row.cells.map(cell => ({ [cell.columnId]: cell.value })),
        ]),
        R.map(R.mergeAll),
      )(data);

      const insertQuery = knex(datasetId).insert(insertData).toString();
      await postgres.query(insertQuery);
    });
    // const head = await s3.headObject(s3Params).promise();
    // console.log(head);
    // const res = await s3.getObject(s3Params).promise();
    // const data = await parseJson(res.Body.toString('utf-8'));

    // baseState = data;
    // layers = baseState.layers ?? initial_layers;
  } catch (e) {
    console.log('error loading dataset from s3', s3Params, e);
  }
};

module.exports = loadS3ToPostgres;
