const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const findMax = require('../../utils/findMax');
const findMin = require('../../utils/findMin');

const aggFuncMap = {
  sum: R.reduce((a, b) => parseInt(a, 10) + parseInt(b, 10), 0),
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

const getColumnValues = (colId, boardData) => {
  const colIndex = boardData.columns.findIndex(col => col._id === colId);
  return R.pipe(
    R.pluck('cells'),
    R.map(row => row[colIndex].value),
  )(boardData.rows);
};

const getValuesOfGrouped = (values, index, boardData) => {
  console.log(values);
  return R.pipe(
    R.filter(
      row =>
        R.intersection(values, R.map(R.prop('value'), row.cells)).length ===
        values.length,
    ),
    R.map(R.prop('cells')),
    R.map(cells => cells[index].value),
  )(boardData.rows);
};

const groupDataset = R.curry((layer, boardData) => {
  const { columns } = boardData;
  const aggregateKeys = Object.keys(layer.columnAggregates);
  const colIdsRepresented = [...layer.groupedBy, ...aggregateKeys];

  const lookupColumn = colId => columns.find(col => col._id === colId);
  const mapColumns = colId => {
    const col = lookupColumn(colId);
    return aggregateKeys.includes(colId)
      ? {
          ...col,
          value: `${layer.columnAggregates[colId]} of ${col.value}`,
        }
      : { ...col };
  };

  const uniqGroupedValues = R.pipe(
    R.map(col => ({
      key: col,
      values: R.uniq(getColumnValues(col, boardData)),
    })),
    R.indexBy(R.prop('key')),
    R.pluck('values'),
  )(layer.groupedBy);

  const lengthOfUniqValues = R.length(R.values(uniqGroupedValues)[0]);
  const mapIndexed = R.addIndex(R.map);

  return {
    ...boardData,
    columns: R.map(mapColumns)(colIdsRepresented),
    rows: mapIndexed((_, index) => ({
      _id: uuidv4(),
      index,
      cells: R.map(colId => ({
        _id: uuidv4(),
        value: uniqGroupedValues[colId]
          ? uniqGroupedValues[colId][index]
          : aggFuncMap[layer.columnAggregates[colId]](
              getValuesOfGrouped(
                uniqGroupedValues['fc8d530e-41d2-43f0-87ce-9e30ab6f8c06'][index],
                index,
                boardData,
              ),
            ),
      }))(colIdsRepresented),
    }))(new Array(lengthOfUniqValues)),
  };
});

module.exports = groupDataset;
