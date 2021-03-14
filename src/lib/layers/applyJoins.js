const R = require('ramda');
const { v4: uuid } = require('uuid');
const getColumnValues = require('../getColumnValuesById');

const mapIndexed = R.addIndex(R.map);

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

  try {
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

    const findCellsFromLookup = row => {
      const joinKey = columns.find(col => col.foreignKeyId)._id;
      const joinValue = row.cells.find(cell => cell.columnId === joinKey)?.value;
      return joinedColumnsLookup[mapPrimaryToForeignKey[joinKey]?.joinedColumnId][
        joinValue
      ];
    };

    const makeRows = R.pipe(
      mapIndexed((row, rowIndex) => ({
        ...row,
        cells: columns.map((col, colIndex) =>
          col.isJoined
            ? findCellsFromLookup(row)?.find(cell => cell.columnId === col._id) ?? {
                _id: uuid(),
                value: '',
                columnId: col._id,
                filterOnInnerJoin: true,
              }
            : findCellByCoordinates([rowIndex, colIndex], boardData),
        ),
      })),
      R.ifElse(
        () => layer.joinType === 'inner',
        R.filter(
          row =>
            row.cells.filter(cell => cell.filterOnInnerJoin !== true).length ===
            row.cells.length,
        ),
        R.identity,
      ),
    );

    return {
      ...boardData,
      columns,
      rows: makeRows(boardData.rows),
    };
  } catch (e) {
    console.log('error in joins', e);
    return boardData;
  }
});

module.exports = applyJoins;
