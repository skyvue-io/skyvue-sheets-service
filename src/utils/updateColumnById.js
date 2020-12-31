const R = require('ramda');

const updateColumnById = R.curry((colId, value, boardData) =>
  R.assoc(
    'columns',
    R.map(col =>
      col._id === colId
        ? {
            ...col,
            value,
          }
        : col,
    )(boardData.columns),
    boardData,
  ),
);

module.exports = updateColumnById;
