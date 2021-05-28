const R = require('ramda');
const { format } = require('sql-formatter');

const knex = require('../../utils/knex');
const makeTableName = require('../makeTableName');

const makeSaveRowsQuery = (datasetId, baseState) => {
  const { unsavedChanges, underlyingColumns } = baseState;

  const table = makeTableName(datasetId);
  const selectQuery = knex.select('id').table(table);

  underlyingColumns.forEach(col => {
    const unsavedChangesForColumn = Object.values(unsavedChanges).filter(
      change => change.columnId === col._id,
    );

    if (unsavedChangesForColumn.length > 0) {
      let baseQuery = `case id`;
      unsavedChangesForColumn.forEach(change => {
        baseQuery += `
          when ''${change.rowId}''
          then ''${change.value}''
        `;
      });
      baseQuery += `else "spectrum"."${datasetId}_working"."${col._id}" end as "${col._id}"`;
      selectQuery.select(knex.raw(baseQuery));
    } else {
      selectQuery.select(col._id);
    }
  });

  console.log(format(selectQuery.toString()));
  const query = `
    UNLOAD ('${selectQuery.toString()}')
    TO 's3://skyvue-datasets/${datasetId}/rows/'
    iam_role '${process.env.REDSHIFT_IAM_ROLE}'
    ALLOWOVERWRITE
    format as CSV
  `;

  console.log(query);

  return query;
};

module.exports = makeSaveRowsQuery;
