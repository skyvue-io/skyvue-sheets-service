const R = require('ramda');
const getColumnValues = require('../getColumnValuesById');
const safeParseNumber = require('../../utils/safeParseNumber');

const attachColumnSummaries = R.curry(boardData => {
  const { columns } = boardData;
  const columnLookup = R.pipe(
    R.pluck('_id'),
    R.reduce(
      (acc, value) => R.assoc(value, getColumnValues(value, boardData), acc),
      {},
    ),
  )(columns);

  const handleCalculation = (...func) => R.pipe(R.map(safeParseNumber), ...func);

  const summary = R.map(colId => {
    const values = columnLookup[colId];
    return {
      columnId: colId,
      uniqueValues: handleCalculation(R.uniq, R.length)(values),
      sum: handleCalculation(R.sum)(values),
      mean: handleCalculation(R.mean)(values),
      min: handleCalculation(R.apply(Math.min))(values),
      max: handleCalculation(R.apply(Math.max))(values),
    };
  })(R.keys(columnLookup));

  return R.assoc('columnSummary', R.indexBy(R.prop('columnId'), summary), boardData);
});

module.exports = attachColumnSummaries;
