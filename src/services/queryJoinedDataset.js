const R = require('ramda');

const loadS3ToPostgres = require('./loadS3ToPostgres');
const makePostgres = require('./postgres');

const knex = require('../utils/knex');
const pgQueryToBoardDataRows = require('../lib/pgQueryToBoardDataRows');
const makeJoinQuery = require('../lib/queries/makeJoinQuery');

const applyJoinToColumns = (columns, joinedColumns, { condition }) =>
  R.pipe(
    R.append(
      R.pipe(
        R.filter(col => R.includes(col._id, condition.select)),
        R.map(R.pipe(R.assoc('isJoined', true))),
      )(joinedColumns),
    ),
    R.flatten,
    R.map(col =>
      condition.on.find(cond => cond.mainColumnId === col._id)
        ? R.assoc(
            'foreignKeyId',
            condition.on.find(cond => cond.mainColumnId === col._id)?.joinedColumnId,
          )(col)
        : col,
    ),
  )(columns);

// plucks data from s3
// ensures that joined tables are in postgres prior to returning joined response
const skip = true;
const queryJoinedDataset = async (datasetId, baseState) => {
  const postgres = await makePostgres();
  const { joins } = baseState?.layers ?? {};

  if (
    skip ||
    !baseState.layerToggles.joins ||
    !joins ||
    !joins?.condition?.datasetId ||
    Object.keys(joins ?? {}).length === 0
  ) {
    return {
      ...baseState,
      rows: pgQueryToBoardDataRows(
        await postgres.query(knex.select().table(`${datasetId}_base`).toString()),
        baseState,
      ),
    };
  }

  const joinedData = await loadS3ToPostgres(joins.condition.datasetId);

  const updatedBaseState = {
    ...baseState,
    columns: applyJoinToColumns(
      baseState.columns,
      joinedData.baseState.columns,
      joins,
    ),
  };

  return {
    ...updatedBaseState,
    rows: pgQueryToBoardDataRows(
      await postgres.query(makeJoinQuery(datasetId, joins)),
      updatedBaseState,
    ),
  };
};

module.exports = queryJoinedDataset;
