const R = require('ramda');
const getColumnValues = require('../getColumnValuesById');

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
        R.map(R.pipe(R.assoc('isJoined', true))),
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

  console.log(columns, joinedData.rows[0]);

  const findCellsFromLookup = row => {
    const joinKey = columns.find(col => col.foreignKeyId)._id;
    const joinValue = row.cells.find(cell => cell.columnId === joinKey)?.value;
    return joinedColumnsLookup[mapPrimaryToForeignKey[joinKey]?.joinedColumnId][
      joinValue
    ];
  };

  // console.log(boardData.rows[0].cells);

  return {
    ...boardData,
    columns,
    rows: boardData.rows.map((row, rowIndex) => ({
      ...row,
      cells: columns.map((col, colIndex) => {
        if (col.isSmartColumn) {
          // console.log(rowIndex, colIndex);
          // console.log(col, findCellByCoordinates([rowIndex, colIndex], boardData));
        }
        return col.isJoined
          ? findCellsFromLookup(row)?.find(cell => cell.columnId === col._id)
          : findCellByCoordinates([rowIndex, colIndex], boardData);
      }),
    })),
  };
});

module.exports = applyJoins;
