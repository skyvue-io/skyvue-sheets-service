const R = require('ramda');

const findColumnIndexById = R.curry((colId, boardData) =>
  boardData.columns.findIndex(col => col._id === colId),
);

module.exports = findColumnIndexById;
