const R = require('ramda');
const knex = require('../../utils/knex');

const makeImportTableQuery = (boardId, content) =>
  knex.schema
    .createTableIfNotExists(`spectrum.${boardId}_append`, table => {
      Object.keys(content[0]).forEach(col => {
        table.string(col);
      });
    })
    .toString()
    .replace('create table', 'create external table') +
  `
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 's3://skyvue-datasets-appends/${boardId}'
  TABLE PROPERTIES (
    'skip.header.line.count'= '1'
  )
  `.trim();

const makeImportQuery = ({ columnMapping, dedupeSettings }, columns, datasetId) => {
  console.log('----- import query -----', columnMapping, dedupeSettings, columns);
  const query = knex.select().table(`${datasetId}_append`);

  columns.forEach(col => {
    const mapping = columnMapping.find(mapping => col._id === mapping.mapTo);
    if (mapping) {
      query.select(`${mapping.importKey} as ${mapping.mapTo}`);
    } else query.select(`null as ${col._id}`);
  });

  // query.union(se)

  console.log(query.toString());

  return query.toString();
};

module.exports = { makeImportQuery, makeImportTableQuery };
