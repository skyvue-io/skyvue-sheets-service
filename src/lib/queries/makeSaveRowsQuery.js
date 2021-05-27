const R = require('ramda');
const { format } = require('sql-formatter');

const knex = require('../../utils/knex');
const makeTableName = require('../makeTableName');

const makeSaveRowsQuery = (datasetId, baseState) => {
  const { unsavedChanges, underlyingColumns } = baseState;

  const columnIdsWithUnsavedChanges = R.pipe(
    R.filter(R.propEq('targetType', 'cell')),
    R.pluck('columnId'),
    R.values,
  )(unsavedChanges);

  const table = makeTableName(datasetId);
  const selectQuery = knex.select().table(table);

  underlyingColumns
    .filter(col => !columnIdsWithUnsavedChanges.includes(col._id))
    .forEach(col => {
      selectQuery.select(col._id);
    });

  underlyingColumns
    .filter(col => columnIdsWithUnsavedChanges.includes(col._id))
    .forEach(col => {
      const unsavedChange = Object.values(unsavedChanges).find(
        change => change.columnId === col._id,
      );

      selectQuery.select(
        knex.raw(
          `case id 
            when ''${unsavedChange.rowId}''
            then ''${unsavedChange.value}''
            else "spectrum"."${datasetId}_working"."${col._id}" 
            end as "${col._id}"`,
        ),
      );
    });

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
