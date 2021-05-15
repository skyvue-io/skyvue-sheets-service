const R = require('ramda');
const sortKeysByColumnOrder = require('./sortKeysByColumnOrder');

const sortDatasetByColumnOrder = (colOrder, baseState) => ({
  ...baseState,
  columns: sortKeysByColumnOrder(colOrder, '_id')(baseState.columns),
  rows: R.map(row => ({
    ...row,
    cells: sortKeysByColumnOrder(colOrder, 'columnId')(row.cells),
  }))(baseState.rows),
});

module.exports = sortDatasetByColumnOrder;
