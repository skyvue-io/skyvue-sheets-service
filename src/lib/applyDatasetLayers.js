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
    dataType: 'string',
    _id: 'a3a6c23e-aea7-4464-89af-fa354c2b8dc5',
    expression:
      // '25176d8a-8a4d-436e-9215-cd03059b3642 + a1fc2674-341d-4124-b22e-fd95f920ee2f + (100 - 3000)',
      'CONCATENATE(c27579ab-a701-46cc-830d-87533abf511e, " ", "!")',
    value: 'test column',
  },
];

const applyDatasetLayers = (layers, boardData) =>
  R.pipe(
    R.assoc('errors', []),
    R.identity, // future, joins
    applySmartColumns(layers.smartColumns),
    applyFilters(layers.filters),
    applyGrouping(layers.groupings),
    applySortings(layers.sortings),
    applyFormatting(formatting),
    updateRowIndeces,
  )(boardData);

module.exports = applyDatasetLayers;
