const R = require('ramda');
const s3 = require('./aws');

const makeRedshift = require('./redshift');

const queryJoinedDataset = require('./queryJoinedDataset');

const loadColumns = require('./loadColumns');

const knex = require('../utils/knex');
const createTableFromColumns = require('../lib/queries/createTableFromColumns');
const applySmartColumns = require('../lib/layers/applySmartColumns');
const buildAggregateLayersQuery = require('../lib/queries/layers/buildAggregateLayersQuery');
const applyFilters = require('../lib/layers/applyFilters');
const makeWorkingTableInsertQuery = require('../lib/queries/makeWorkingTableInsertQuery');
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

const temp_layers = {
  joins: {
    joinType: 'left',
    condition: {
      datasetId: '6096eaf85bfb143205aba59c',
      select: ['0843dae0-18ed-42ab-9918-b3856f694c46'],
      on: [
        {
          mainColumnId: 'e31e4e49-8f50-48de-93f2-6d80602f2908',
          joinedColumnId: '904e6a7e-60ce-4cf2-b16d-9f2c2c27576f',
        },
      ],
    },
  },
  groupings: {
    groupedBy: ['f27ef665-e16f-4af1-846b-a6f8e1c9620e'],
    // todo this needs to be an array so you can see different aggregations on the same column
    columnAggregates: {
      '4271f58e-4182-4506-8555-2375d2c42fa7': 'sum',
    },
  },
  sortings: [{ key: 'f27ef665-e16f-4af1-846b-a6f8e1c9620e', direction: 'asc' }],
};

// assumes data is already in postgres
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

  // if (!columnsAndLayers) {
  //   console.log('columns and layers loaded from s3 look like this', {
  //     columns,
  //     layers: rest.layers,
  //   });
  // }

  const layers_ = rest.layers ?? initial_layers;
  const layers = {
    ...layers_,
    // ...temp_layers,
  };

  const joinedDatasetColumns = layers.joins?.condition?.datasetId
    ? await loadColumns(layers.joins.condition.datasetId)
    : undefined;

  const layerVisibilityTable = {
    joins:
      layers.joins &&
      Object.keys(layers.joins).length > 0 &&
      layerToggles.joins === true,
    filters: (layers.filters ?? []).length > 0 && layerToggles.filters === true,
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
