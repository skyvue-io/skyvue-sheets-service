const R = require('ramda');

const updateCellById = R.curry((cellId, value, boardData) =>
  R.assoc(
    'rows',
    R.map(row => ({
      ...row,
      index: row.index - 1,
      cells: row.cells.map(cell =>
        cell._id === cellId ? { ...cell, value } : cell,
      ),
    }))(boardData.rows),
    boardData,
  ),
);

module.exports = updateCellById;
