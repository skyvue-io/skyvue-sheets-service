const R = require('ramda');

const predicateMap = {
  // eslint-disable-next-line eqeqeq
  equals: (a, b) => a == b,
  // eslint-disable-next-line eqeqeq
  notEquals: (a, b) => a != b,
  contains: (a, b) => a.includes(b),
  lessThan: (a, b) => a < b,
  lessThanEqualTo: (a, b) => a <= b,
  greaterThan: (a, b) => a > b,
  greaterThanEqualTo: (a, b) => a >= b,
  dateBetween: R.__,
};

const processRow = R.curry((logicalRules, boardData, row) => {
  if (logicalRules.length === 1) return boardData;
  if (boardData.layerToggles.filters === false) return boardData;
  const topLevelOperator = logicalRules[0];
  const topLevelConditions = logicalRules
    .slice(1, logicalRules.length)
    .filter(rule => !Array.isArray(rule));

  const nestedConditions = logicalRules.find(rule => Array.isArray(rule));

  const cellValueByColumnIndex = colId =>
    boardData.columns.findIndex(col => col._id === colId);

  const processCondition = cond =>
    predicateMap[cond.predicateType](
      row.cells[cellValueByColumnIndex(cond.key)].value,
      cond.value,
    );

  if (topLevelConditions.length > 0) {
    if (topLevelOperator === 'AND') {
      if (R.all(processCondition)(topLevelConditions)) {
        if (!nestedConditions) return true;
        return processRow(nestedConditions, boardData, row);
      }
      return false;
    }
    if (topLevelOperator === 'OR') {
      if (R.any(processCondition)(topLevelConditions)) return true;
      return false;
    }
  }
});

const applyFilters = R.curry((logicalRules, boardData) => ({
  ...boardData,
  rows:
    logicalRules.length > 0
      ? R.filter(processRow(logicalRules, boardData))(boardData.rows)
      : boardData.rows,
}));

module.exports = applyFilters;
