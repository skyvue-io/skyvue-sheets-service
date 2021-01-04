const R = require('ramda');
const updateCellById = require('../utils/updateCellById');
const updateColumnById = require('../utils/updateColumnById');
const handleColumnTimeTravel = require('./handleColumnTimeTravel');

const makeBoardDataFromVersion = (
  { targetId, changeTarget, prevValue, newValue, secondaryValue },
  direction,
  boardData,
  removedColumns,
) => {
  const targetValue = direction === 'undo' ? prevValue : newValue;

  if (changeTarget === 'cell') {
    return updateCellById(targetId, targetValue, boardData);
  }
  if (changeTarget === 'column') {
    if (secondaryValue) {
      const removedColumn = removedColumns[targetId];
      const columns = handleColumnTimeTravel(targetId, targetValue, boardData);

      return {
        ...columns,
        rows:
          targetValue && secondaryValue.changeTarget === 'cells'
            ? boardData.rows.map((row, index) => ({
                ...row,
                cells: [...row.cells, removedColumn[index]],
              }))
            : boardData.rows.map((row, index) => ({
                ...row,
                cells: row.cells.filter(
                  (cell, index) =>
                    index !==
                    boardData.columns.findIndex(col => col._id === targetId),
                ),
              })),
      };
    }
    return updateColumnById(targetId, targetValue, boardData);
  }

  if (changeTarget === 'row') {
    return {
      ...boardData,
      rows: targetValue
        ? R.insert(targetValue.index - 1, targetValue, boardData.rows)
        : boardData.rows.filter(row => row._id !== targetId),
    };
  }

  return boardData;
};

module.exports = makeBoardDataFromVersion;
