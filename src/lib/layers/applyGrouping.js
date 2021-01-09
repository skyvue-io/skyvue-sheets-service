const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const findMax = require('../../utils/findMax');
const findMin = require('../../utils/findMin');
const findRowsWithValues = require('../queries/findRowsWithValues');

const aggFuncMap = {
  sum: R.reduce((a, b) => {
    console.log(a, b);
    return parseFloat(a) + parseFloat(b);
  }, 0),
  mean: R.mean,
  median: R.median,
  countDistinct: arr => [...new Set(arr)].length,
  count: R.length,
  max: findMax,
  min: findMin,
  stdev: array => {
    const n = array.length;
    const mean = array.reduce((a, b) => a + b) / n;
    return Math.sqrt(array.map(x => (x - mean) ** 2).reduce((a, b) => a + b) / n);
  },
};

const getColumnValues = (colId, boardData) =>
  R.pipe(
    R.pluck('cells'),
    R.map(row => row[boardData.columns.findIndex(col => col._id === colId)].value),
  )(boardData.rows);

const groupDataset = R.curry((layer, boardData) => {
  if (R.length(R.keys(layer)) === 0) return boardData;

  const { columns } = boardData;
  const { groupedBy, columnAggregates } = layer;
  const aggregateKeys = Object.keys(columnAggregates);
  const colIdsRepresented = [...groupedBy, ...aggregateKeys];

  const uniqGroupedValues = R.pipe(
    R.map(col => ({
      key: col,
      values: R.uniq(getColumnValues(col, boardData)),
    })),
    R.indexBy(R.prop('key')),
    R.pluck('values'),
  )(groupedBy);

  const mapColumns = colId => {
    const col = columns.find(col => col._id === colId);
    return aggregateKeys.includes(colId)
      ? {
          ...col,
          value: `${columnAggregates[colId]} of ${col.value}`,
        }
      : { ...col };
  };

  const mapGroupedCells = R.curry((rowIndex, colId) => ({
    _id: uuidv4(),
    value: uniqGroupedValues[colId]
      ? uniqGroupedValues[colId][rowIndex]
      : aggFuncMap[columnAggregates[colId]](
          findRowsWithValues(
            R.values(R.map(groupedCol => groupedCol[rowIndex])(uniqGroupedValues)),
            boardData,
          ),
        ),
  }));

  const lengthOfUniqValues = R.length(R.values(uniqGroupedValues)[0]);
  const mapIndexed = R.addIndex(R.map);

  return {
    ...boardData,
    columns: R.map(mapColumns)(colIdsRepresented),
    rows: mapIndexed((_, index) => ({
      _id: uuidv4(),
      index,
      cells: R.map(mapGroupedCells(index))(colIdsRepresented),
    }))(new Array(lengthOfUniqValues)),
  };
});

module.exports = groupDataset;
