const R = require('ramda');

const loadS3ToPostgres = require('./loadS3ToPostgres');
const makePostgres = require('./postgres');

const knex = require('../utils/knex');
const pgQueryToBoardData = require('../lib/pgQueryToBoardData');
const makeJoinQuery = require('../lib/queries/makeJoinQuery');

// plucks data from s3
// ensures that joined tables are in postgres prior to returning joined response

const joinLayer = {
  joinType: 'left',
  condition: {
    datasetId: '607c620690fe20b871ae7083',
    select: ['dcdb77b3-bf37-4137-b63a-56118428be03'],
    on: [
      {
        mainColumnId: 'bf9a6095-1b92-4149-8404-ce20af6ecf50',
        joinedColumnId: '52b45770-329e-423b-8575-4202969234af',
      },
    ],
  },
};

// const applyJoinToColumns = (columns, { condition }) =>
//   R.pipe(
//     R.append(
//       R.flatten,
//       R.pipe(
//         R.map(col => condition.select)
//       )([...condition.on,])
//     )
//     R.append(
//       R.pipe(
//         R.filter(col => R.includes(col._id, condition.select)),
//         R.map(R.pipe(R.assoc('isJoined', true))),
//       )(joinedData.columns),
//     ),
//     R.flatten,
//     R.map(col =>
//       condition.on.find(cond => cond.mainColumnId === col._id)
//         ? R.assoc(
//             'foreignKeyId',
//             condition.on.find(cond => cond.mainColumnId === col._id)?.joinedColumnId,
//           )(col)
//         : col,
//     ),
//   )(columns);

const queryJoinedDataset = async (datasetId, baseState) => {
  const postgres = await makePostgres();
  const { joins } = { joins: joinLayer }; // baseState?.layers ?? {};

  if (
    // add condition for layertoggles
    !joins ||
    !joins?.condition?.datasetId ||
    !joinLayer.condition.datasetId ||
    Object.keys(joins ?? {}).length === 0
  ) {
    const res = await postgres.query(
      knex.select().table(`${datasetId}_base`).toString(),
    );

    return pgQueryToBoardData(res, baseState);
  }

  await loadS3ToPostgres(joinLayer.condition.datasetId);

  return pgQueryToBoardData(
    await postgres.query(makeJoinQuery(datasetId, joinLayer)),
    baseState,
  );
};

module.exports = queryJoinedDataset;
