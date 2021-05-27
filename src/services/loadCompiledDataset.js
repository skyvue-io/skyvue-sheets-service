const R = require('ramda');

const makeRedshift = require('./redshift');

const loadColumns = require('./loadColumns');

const queryResponseToBoardDataRows = require('../lib/queryResponseToBoardDataRows');
const reconstructGroupedBoardData = require('../lib/reconstructGroupedBoardData');
const makeQueryFromLayers = require('../lib/makeQueryFromLayers');
const applyJoinToColumns = require('../lib/layers/applyJoinToColumns');

const initial_layers = {
  joins: {}, // {}
  filters: [], // filterLayer, // []
  groupings: {}, // groupLayer, // {}
  smartColumns: [],
  sortings: [], // []
  formatting: [],
};

// assumes data is already in redshift
const loadCompiledDataset = async (
  datasetId,
  columnsAndLayers,
  { onlyHead = false } = {},
) => {
  const redshift = await makeRedshift();
  const {
    columns,
    underlyingColumns,
    baseColumns,
    layerToggles,
    deletedObjects,
    ...rest
  } = columnsAndLayers ?? (await loadColumns(datasetId));

  const layers = rest.layers ?? initial_layers;

  const joinedDatasetColumns = layers.joins?.condition?.datasetId
    ? await loadColumns(layers.joins.condition.datasetId)
    : undefined;

  const layerVisibilityTable = {
    joins:
      layers.joins &&
      Object.keys(layers.joins).length > 0 &&
      layerToggles.joins === true,
    sortings: (layers.sortings ?? []).length > 0,
    groupings:
      layers.groupings &&
      Object.keys(layers.groupings).length > 0 &&
      layerToggles.groupings === true,
  };

  const applyIfVisible = (layerKey, onTrue) =>
    R.ifElse(() => layerVisibilityTable[layerKey], onTrue, R.identity);

  const newColumns = R.pipe(
    applyIfVisible(
      'joins',
      applyJoinToColumns(joinedDatasetColumns?.columns ?? {}, layers.joins),
    ),
  )(baseColumns ?? columns);

  if (onlyHead) {
    return {
      _id: datasetId,
      columns: newColumns,
      layers,
    };
  }

  const baseState = {
    ...rest,
    layers,
    layerToggles,
    deletedObjects,
    columns: newColumns,
    underlyingColumns: newColumns,
    baseColumns: baseColumns ?? columns,
    rows: queryResponseToBoardDataRows(
      await redshift.query(
        makeQueryFromLayers(
          layerVisibilityTable.groupings,
          applyIfVisible,
          datasetId,
          {
            columns: newColumns,
            layers,
          },
        ),
      ),
      newColumns,
    ),
  };
  // todo apply layers to column data

  return applyIfVisible(
    'groupings',
    reconstructGroupedBoardData(layers.groupings),
  )(baseState);
};

module.exports = loadCompiledDataset;
