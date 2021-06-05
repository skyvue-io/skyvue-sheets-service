const R = require('ramda');

const makeTableName = require('../makeTableName');
const knex = require('../../utils/knex');

const makeImportTableQuery = (boardId, content) =>
  knex.schema
    .createTable(`spectrum.${boardId}_append`, table => {
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
  const query = knex.select().table(`spectrum.${datasetId}_append`);
  query.select(
    knex.raw(
      'md5(cast (random() * 100 as int) || cast(random() * 100 as int) || TIMEOFDAY()) as id',
    ),
  );

  columns.forEach(col => {
    const mapping = columnMapping.find(mapping => col._id === mapping.mapTo);
    if (mapping) {
      query.select(`${mapping.importKey} as ${mapping.mapTo}`);
    } else {
      query.select(knex.raw(`null as "${col._id}"`));
    }
  });

  query.union([
    knex
      .select('id', ...columns.map(col => knex.raw(`"${col._id}"::varchar(max)`))) // cast everything as a string until validation is built
      .from(makeTableName(datasetId)),
  ]);

  return knex
    .select('id', ...columns.map(col => col._id))
    .from(
      knex
        .with('query', query)
        .select('*')
        .rank(
          'rank',
          ['id'],
          dedupeSettings.dedupeOn.length > 0 ? dedupeSettings.dedupeOn : ['id'],
        )
        .from('query'),
    )
    .where({ rank: 1 })
    .toString();
};

module.exports = { makeImportQuery, makeImportTableQuery };
