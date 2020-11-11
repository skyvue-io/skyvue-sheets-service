const R = require('ramda');

const boardDataToCSVReadableJSON = boardData => {
  const mapIndexed = R.addIndex(R.map);
  const columns = R.pipe(R.values, R.pluck('value'))(boardData.columns);

  const mapToKeyValue = row => {
    const values = R.pluck('value')(row.cells);
    return R.pipe(
      mapIndexed((value, index) => ({
        [columns[index]]: values[index],
      })),
      R.mergeAll,
    )(row.cells);
  };

  return R.map(mapToKeyValue)(boardData.rows);
};

module.exports = boardDataToCSVReadableJSON;
