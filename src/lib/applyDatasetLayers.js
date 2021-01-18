const R = require('ramda');
const applyFilters = require('./layers/applyFilters');
const applyFormatting = require('./layers/applyFormatting');
const applyGrouping = require('./layers/applyGrouping');
const applySmartColumns = require('./layers/applySmartColumns');
const applySortings = require('./layers/applySortings');

const formatting = [
  { colId: '84de2dbb-8d15-42f0-a0a9-43aebc0d4aa3', format: 'currency' },
];

const mapIndexed = R.addIndex(R.map);
const updateRowIndeces = boardData =>
  R.assoc(
    'rows',
    mapIndexed((row, index) => R.assoc('index', index + 1, row))(boardData.rows),
    boardData,
  );

const smartColumns = [
  {
    expression:
      'col(25176d8a-8a4d-436e-9215-cd03059b3642) + col(a1fc2674-341d-4124-b22e-fd95f920ee2f)',
    columnName: 'test column',
  },
];

const applyDatasetLayers = (layers, boardData) =>
  R.pipe(
    R.identity, // future, joins
    applyFilters(layers.filters),
    applyGrouping(layers.groupings),
    applySmartColumns(smartColumns),
    applySortings(layers.sortings),
    applyFormatting(formatting),
    updateRowIndeces,
  )(boardData);

module.exports = applyDatasetLayers;
