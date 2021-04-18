const R = require('ramda');
const knex = require('../../utils/knex');

const makeJoinQuery = (datasetId, joinLayer) => {
  const baseTable = `${datasetId}_base`;
  const { joinType } = joinLayer;
  const joinedTable = `${joinLayer.condition.datasetId}_base`;

  const joinOn = R.pipe(
    R.map(on => [
      `${baseTable}.${on.mainColumnId}`,
      `${joinedTable}.${on.joinedColumnId}`,
    ]),
    R.flatten(),
  )(joinLayer.condition.on);

  return R.pipe(
    query =>
      joinType === 'left'
        ? query.leftJoin(joinedTable, ...joinOn).clone()
        : query.clone(),
    query =>
      joinType === 'right'
        ? query.rightJoin(joinedTable, ...joinOn).clone()
        : query.clone(),
    query =>
      joinType === 'inner'
        ? query.innerJoin(joinedTable, ...joinOn).clone()
        : query.clone(),
    query =>
      joinType === 'full'
        ? query.fullOuterJoin(joinedTable, ...joinOn).clone()
        : query.clone(),
    query =>
      query.select(
        `${baseTable}.*`,
        ...joinLayer.condition.select.map(colId => `${joinedTable}.${colId}`),
      ),
    query => query.toString(),
  )(knex(baseTable));
};

module.exports = makeJoinQuery;
