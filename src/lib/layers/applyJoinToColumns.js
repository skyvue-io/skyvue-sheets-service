const R = require('ramda');

const applyJoinToColumns = R.curry((joinedDatasetColumns, { condition }, columns) =>
  R.pipe(
    R.append(
      R.pipe(
        R.filter(col => R.includes(col._id, condition.select)),
        R.map(R.pipe(R.assoc('isJoined', true))),
      )(joinedDatasetColumns),
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
  )(columns),
);

module.exports = applyJoinToColumns;
