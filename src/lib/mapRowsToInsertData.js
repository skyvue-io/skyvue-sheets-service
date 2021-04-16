const R = require('ramda');

const mapRowsToInsertData = columns =>
  R.pipe(
    R.map(row => ({
      ...row,
      cells: row.cells.map((cell, index) => ({
        ...cell,
        columnId: cell.columnId ?? columns[index]?._id,
      })),
    })),
    R.map(row => [
      { _id: row._id },
      ...row.cells.map(cell => ({ [cell.columnId]: cell.value })),
    ]),
    R.map(R.mergeAll),
  );

module.exports = mapRowsToInsertData;
