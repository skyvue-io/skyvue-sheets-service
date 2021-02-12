const R = require('ramda');
const {
  isSameDay,
  isSameWeek,
  isSameMonth,
  isSameYear,
  isEqual,
  isBefore,
  isAfter,
  differenceInDays,
} = require('date-fns');

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

  sameDay: (a, b) => isSameDay(new Date(a), new Date(b)),
  sameWeek: (a, b) => isSameWeek(new Date(a), new Date(b)),
  sameMonth: (a, b) => isSameMonth(new Date(a), new Date(b)),
  sameYear: (a, b) => isSameYear(new Date(a), new Date(b)),
  equals_date: (a, b) => isEqual(new Date(a), new Date(b)),
  notEquals_date: (a, b) => !isEqual(new Date(a), new Date(b)),
  lessThan_date: (a, b) =>
    differenceInDays(new Date(a), new Date(b)) < 0 ||
    isBefore(new Date(a), new Date(b)),
  lessThanEqualTo_date: (a, b) =>
    isBefore(new Date(a), new Date(b)) || isSameDay(new Date(a), new Date(b)),
  greaterThan_date: (a, b) =>
    differenceInDays(new Date(a), new Date(b)) > 0 &&
    isAfter(new Date(a), new Date(b)),
  greaterThanEqualTo_date: (a, b) =>
    isAfter(new Date(a), new Date(b)) || isSameDay(new Date(a), new Date(b)),
  dateBetween: R.__,
};

const processRow = R.curry((logicalRules, boardData, row) => {
  if (logicalRules.length === 1) return boardData;
  if (!boardData.layerToggles?.filters) return boardData;
  const topLevelOperator = logicalRules[0];
  const topLevelConditions = logicalRules
    .slice(1, logicalRules.length)
    .filter(rule => !Array.isArray(rule));

  const nestedConditions = logicalRules.find(rule => Array.isArray(rule));

  const cellValueByColumnIndex = colId =>
    boardData.columns.findIndex(col => col._id === colId);

  const processCondition = cond =>
    predicateMap[cond.predicateType]?.(
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
