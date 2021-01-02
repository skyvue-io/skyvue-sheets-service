const R = require('ramda');
const updateCellById = require('../utils/updateCellById');
const updateColumnById = require('../utils/updateColumnById');
const handleColumnTimeTravel = require('./handleColumnTimeTravel');

const makeBoardDataFromVersion = (
  { targetId, changeTarget, prevValue, newValue, secondaryValue },
  direction,
  boardData,
) => {
  const targetValue = direction === 'undo' ? prevValue : newValue;

  if (changeTarget === 'cell') {
    return updateCellById(targetId, targetValue, boardData);
  }
  if (changeTarget === 'column') {
    if (secondaryValue) {
      return {
        ...boardData,
        columns: handleColumnTimeTravel(targetId, targetValue, boardData),
        rows:
          targetValue && secondaryValue.changeTarget === 'cells'
            ? boardData.rows.map(row => ({
                ...row,
                targetValue,
              }))
            : boardData.rows,
      };
    }
    return updateColumnById(targetId, targetValue, boardData);
  }

  return boardData;
};

module.exports = makeBoardDataFromVersion;
