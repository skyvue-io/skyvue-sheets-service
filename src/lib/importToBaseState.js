const R = require('ramda');
const { v4: uuid } = require('uuid');

const importToBaseState = ({
  columnMapping,
  dedupeSettings,
  importData,
  baseState,
}) => {
  const appendId = uuid();
  const columnIds = R.pluck('_id', baseState.columns);

  const findMappedKey = colId =>
    R.pipe(R.find(R.propEq('mapTo', colId)), R.prop('importKey'))(columnMapping);

  const rowsToAppend = importData.map((row, index) => ({
    _id: uuid(),
    index,
    appendId,
    cells: columnIds.map((colId, index) => ({
      _id: uuid(),
      index,
      columnId: colId,
      // select key from row
      value: row[findMappedKey(colId)] ?? '',
    })),
  }));

  const newBaseState = {
    ...baseState,
    rows: [...baseState.rows, ...rowsToAppend],
  };

  // dedupe that shit
  return newBaseState;
};

module.exports = importToBaseState;
