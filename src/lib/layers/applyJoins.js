const R = require('ramda');

const getColumnValues = (colId, boardData) =>
  R.pipe(
    R.pluck('cells'),
    R.map(row => row[boardData.columns.findIndex(col => col._id === colId)].value),
  )(boardData.rows);

const findCellByCoordinates = R.curry(
  ([rowIndex, columnIndex], boardData) =>
    boardData.rows.find((_, index) => index === rowIndex)?.cells[columnIndex],
);

const findRowByCellValue = (value, boardData) =>
  boardData.rows.find(row => row.cells.find(cell => cell.value === value));

const applyJoins = R.curry((joinedData_, layer, boardData) => {
  if (
    !boardData.layerToggles?.joins ||
    R.keys(layer).length === 0 ||
    R.keys(joinedData_).length === 0
  )
    return boardData;

  const { condition } = layer;
  const { on } = condition;

  const joinedData = joinedData_[condition.datasetId];
  const joinedColumnsLookup = R.pipe(
    R.reduce(
      (acc, value) => R.assoc(value, getColumnValues(value, joinedData), acc),
      {},
    ),
    R.map(
      R.reduce(
        (acc, value) =>
          R.assoc(value, findRowByCellValue(value, joinedData)?.cells ?? [], acc),
        {},
      ),
    ),
  )(R.map(R.prop('joinedColumnId'), on));

  const mapPrimaryToForeignKey = R.indexBy(R.prop('mainColumnId'))(on);

  const columns = R.pipe(
    R.append(
      R.pipe(
        R.filter(col => R.includes(col._id, condition.select)),
        R.map(R.pipe(R.assoc('isJoined', true), R.assoc('value', on[0].as))),
      )(joinedData.columns),
    ),
    R.flatten,
    R.map(col =>
      on.find(cond => cond.mainColumnId === col._id)
        ? R.assoc(
            'foreignKeyId',
            on.find(cond => cond.mainColumnId === col._id)?.joinedColumnId,
          )(col)
        : col,
    ),
  )(boardData.columns);

  const findCellsFromLookup = row => {
    const joinKey = columns.find(col => col.foreignKeyId)._id;
    const joinValue = row.cells.find(cell => cell.columnId === joinKey)?.value;
    return joinedColumnsLookup[mapPrimaryToForeignKey[joinKey]?.joinedColumnId][
      joinValue
    ];
  };

  return {
    ...boardData,
    columns,
    rows: boardData.rows.map((row, rowIndex) => ({
      ...row,
      cells: columns.map((col, colIndex) =>
        col.isJoined
          ? findCellsFromLookup(row)?.find(cell => cell.columnId === col._id)
          : findCellByCoordinates([rowIndex, colIndex], boardData),
      ),
    })),
  };
});

module.exports = applyJoins;
