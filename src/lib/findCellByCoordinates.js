const R = require('ramda');

const findCellValueByCoordinates = R.curry(
  ([rowIndex, columnIndex], boardData) =>
    boardData.rows.find((_, index) => index === rowIndex)?.cells[columnIndex],
);

module.exports = findCellValueByCoordinates;
