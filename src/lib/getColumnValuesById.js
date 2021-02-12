const R = require('ramda');

const getColumnValues = (colId, boardData) =>
  R.pipe(
    R.pluck('cells'),
    R.map(row => {
      const colIndex = boardData.columns.findIndex(col => col._id === colId);
      return row[colIndex]?.value ?? '';
    }),
  )(boardData.rows);

module.exports = getColumnValues;
