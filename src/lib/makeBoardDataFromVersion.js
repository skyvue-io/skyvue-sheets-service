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
      console.log(removedColumn);
      return {
        ...boardData,
        columns: handleColumnTimeTravel(targetId, targetValue, boardData),
        rows:
          targetValue && secondaryValue.changeTarget === 'cells'
            ? boardData.rows.map((row, index) => ({
                ...row,
                cells: [...row.cells, removedColumn[index]],
              }))
            : boardData.rows,
      };
    }
    return updateColumnById(targetId, targetValue, boardData);
  }

  return boardData;
};

module.exports = makeBoardDataFromVersion;
