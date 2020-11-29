const R = require('ramda');
const applyFilters = require('./layers/applyFilters');
const applyGrouping = require('./layers/applyGrouping');
const applySmartColumns = require('./layers/applySmartColumns');
const applySortings = require('./layers/applySortings');

const applyDatasetLayers = (layers, boardData) =>
  R.pipe(
    R.identity, // future, joins
    applyFilters(layers.filters),
    applyGrouping(layers.groupings),
    applySmartColumns(layers.smartColumns),
    applySortings(layers.sortings),
    R.identity, // future, formatting,
  )(boardData);

module.exports = applyDatasetLayers;
