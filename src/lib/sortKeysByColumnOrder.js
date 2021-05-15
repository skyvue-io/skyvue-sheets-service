const R = require('ramda');

const sortKeysByColumnOrder = (columnIds, selector = 'id') =>
  R.sortBy(R.pipe(R.prop(selector), R.indexOf(R.__, columnIds)));

module.exports = sortKeysByColumnOrder;
