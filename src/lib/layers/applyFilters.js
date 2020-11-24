const R = require('ramda');
const predicateMap = require('./predicateMap');

const processRow = R.curry((logicalRules, boardData, row) => {
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
