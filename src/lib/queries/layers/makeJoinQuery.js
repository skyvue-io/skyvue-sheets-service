const R = require('ramda');
const makeTableName = require('../../makeTableName');

const makeJoinQuery = R.curry((datasetId, joinLayer, knex) => {
  const { joinType, condition } = joinLayer;
  const baseTable = makeTableName(datasetId);
  const joinedTable = makeTableName(condition.datasetId);

  const joinOn = R.pipe(
    R.map(on => [
      `${baseTable}.${on.mainColumnId}`,
      `${joinedTable}.${on.joinedColumnId}`,
    ]),
    R.flatten(),
  )(condition.on);

  return R.pipe(
    knex =>
      joinType === 'left'
        ? knex.leftJoin(joinedTable, ...joinOn).clone()
        : knex.clone(),
    knex =>
      joinType === 'right'
        ? knex.rightJoin(joinedTable, ...joinOn).clone()
        : knex.clone(),
    knex =>
      joinType === 'inner'
        ? knex.innerJoin(joinedTable, ...joinOn).clone()
        : knex.clone(),
    knex =>
      joinType === 'full'
        ? knex.fullOuterJoin(joinedTable, ...joinOn).clone()
        : knex.clone(),
    knex =>
      knex.select(
        `${baseTable}.*`,
        ...joinLayer.condition.select.map(colId => `${joinedTable}.${colId}`),
      ),
  )(knex.clone());
});

module.exports = makeJoinQuery;
