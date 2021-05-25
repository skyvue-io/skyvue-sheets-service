const knex = require('../../utils/knex');
const makeTableName = require('../makeTableName');

const makeSaveRowsQuery = (datasetId, unsavedChanges, columns) => {
  const table = makeTableName(datasetId);
  const selectQuery = knex(table).select().toString();

  const query = ```
    UNLOAD ('${selectQuery}')
    TO 's3://skyvue-datasets/${datasetId}/rows/'
    iam_role 'arn:aws:iam::082311462302:role/redshift-s3-and-athena-full-access'

    format as CSV
  ```;

  console.log(query);
  return query;
};

module.exports = makeSaveRowsQuery;
