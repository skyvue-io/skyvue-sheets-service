const R = require('ramda');

const findRowsWithValues = (values, boardData) =>
  R.pipe(
    R.filter(
      row =>
        R.intersection(values, R.map(R.prop('value'), row.cells)).length ===
        values.length,
    ),
    R.map(row => R.map(R.assoc('rowId', row._id))(row.cells)),
  )(boardData.rows);

module.exports = findRowsWithValues;
