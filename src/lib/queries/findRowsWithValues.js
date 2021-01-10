const R = require('ramda');

const mapIndexed = R.addIndex(R.map);
const findRowsWithValues = R.curry((values, boardData) =>
  R.pipe(
    R.filter(
      row =>
        R.intersection(values, R.map(R.prop('value'), row.cells)).length ===
        values.length,
    ),
    R.map(row =>
      mapIndexed((_row, index) =>
        R.pipe(
          R.assoc('rowId', row._id),
          R.assoc(
            'colId',
            boardData.columns.find((col, index_) => index_ === index)?._id,
          ),
        )(_row),
      )(row.cells),
    ),
  )(boardData.rows),
);

module.exports = findRowsWithValues;
