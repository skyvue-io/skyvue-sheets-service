const R = require('ramda');
const { v4: uuid } = require('uuid');
const dedupeDataset = require('./dedupeDataset');

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

  if (!dedupeSettings || dedupeSettings.dedupeOn.length === 0) {
    return newBaseState;
  }

  const mappedDedupeSettings = R.assoc(
    'dedupeOn',
    R.map(dedupeKey => columnMapping.find(R.propEq('importKey', dedupeKey))?.mapTo)(
      dedupeSettings.dedupeOn,
    ),
  )(dedupeSettings);

  return dedupeDataset(mappedDedupeSettings, newBaseState);
};

module.exports = importToBaseState;
