const R = require('ramda');
const applyFilters = require('./layers/applyFilters');
const applyFormatting = require('./layers/applyFormatting');
const applyGrouping = require('./layers/applyGrouping');
const applySmartColumns = require('./layers/applySmartColumns');
const applySortings = require('./layers/applySortings');

const smartColumns = [
  {
    _id: 'test',
    columns: [
      '84de2dbb-8d15-42f0-a0a9-43aebc0d4aa3',
      '8ebadc7a-fdaa-4ceb-9e2a-ccc7f9c97886',
    ],
    predicate: 'concat',
  },
];

const formatting = [
  { colId: '84de2dbb-8d15-42f0-a0a9-43aebc0d4aa3', format: 'currency' },
];

const mapIndexed = R.addIndex(R.map);
const updateRowIndeces = boardData =>
  R.assoc(
    'rows',
    mapIndexed((row, index) => R.assoc('index', index, row))(boardData.rows),
    boardData,
  );

const applyDatasetLayers = (layers, boardData) =>
  R.pipe(
    R.identity, // future, joins
    applyFilters(layers.filters),
    applyGrouping(layers.groupings),
    applySmartColumns(layers.smartColumns),
    applySortings(layers.sortings),
    applyFormatting(formatting),
    updateRowIndeces,
  )(boardData);

module.exports = applyDatasetLayers;
