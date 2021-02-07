const R = require('ramda');

const getColumnValues = (colId, boardData) =>
  R.pipe(
    R.pluck('cells'),
    R.map(row => row[boardData.columns.findIndex(col => col._id === colId)].value),
  )(boardData.rows);

module.exports = getColumnValues;
