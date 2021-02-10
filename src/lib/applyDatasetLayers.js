const R = require('ramda');

const applyFilters = require('./layers/applyFilters');
const applyGrouping = require('./layers/applyGrouping');
const applySmartColumns = require('./layers/applySmartColumns');
const applySortings = require('./layers/applySortings');
const applyJoins = require('./layers/applyJoins');

const attachColumnSummaries = require('./layers/attachColumnSummaries');

const getColumnValuesById = require('./getColumnValuesById');

const mapIndexed = R.addIndex(R.map);
const updateRowIndeces = boardData =>
  R.assoc(
    'rows',
    mapIndexed((row, index) => R.assoc('index', index + 1, row))(boardData.rows),
    boardData,
  );

const attachColumnIdToCells = boardData =>
  R.assoc(
    'rows',
    boardData.rows.map(row => ({
      ...row,
      cells: row.cells.map((cell, index) => ({
        ...cell,
        columnId: boardData.columns[index]?._id,
      })),
    })),
    boardData,
  );

const attachIsUnique = boardData =>
  R.assoc(
    'columns',
    R.map(col =>
      R.assoc(
        'isUnique',
        new Set(getColumnValuesById(col._id, boardData)).size ===
          boardData.rows.length,
      )(col),
    )(boardData.columns),
    boardData,
  );

const applyDatasetLayers = (_id, layers, joinedData, boardData) =>
  R.pipe(
    R.assoc('errors', []),
    R.assoc('_id', _id),
    attachIsUnique,
    attachColumnIdToCells,
    applySmartColumns(layers.smartColumns),
    applyJoins(joinedData, layers.joins),
    applyFilters(layers.filters),
    attachColumnSummaries,
    applyGrouping(layers.groupings),
    applySortings(layers.sortings),
    updateRowIndeces,
  )(boardData);

module.exports = applyDatasetLayers;
