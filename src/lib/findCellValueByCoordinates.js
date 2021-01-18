const R = require('ramda');

const findCellValueByCoordinates = R.curry(
  ([x, y], boardData) =>
    boardData.rows.find((_, index) => index === x)?.cells[y]?.value,
);

module.exports = findCellValueByCoordinates;
