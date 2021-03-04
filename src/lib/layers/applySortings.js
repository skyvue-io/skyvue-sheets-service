const _ = require('lodash');
const R = require('ramda');
const safeParseNumber = require('../../utils/safeParseNumber');

const applySortings = R.curry((sortLayer, boardData) => {
  if (sortLayer.length === 0) return boardData;
  const indecesOfSortKeys = sortLayer.map(sorting =>
    boardData.columns.findIndex(col => col._id === sorting.key),
  );

  const keysMappedToSelectorFunctions = indecesOfSortKeys.map(index =>
    R.pipe(R.path(['cells', index]), R.prop('value'), safeParseNumber),
  );

  const sortedRows = _.orderBy(
    boardData.rows,
    keysMappedToSelectorFunctions,
    R.map(R.prop('direction'), sortLayer),
  );

  return R.assoc('rows', sortedRows, boardData);
});

module.exports = applySortings;
