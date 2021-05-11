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
};

// assumes data is already in postgres
const loadCompiledDataset = async (datasetId, columnsAndLayers) => {
  const redshift = await makeRedshift();
  const { columns, layerToggles, ...rest } =
    columnsAndLayers ?? (await loadColumns(datasetId));

  const layers_ = rest.layers ?? initial_layers;
  const layers = {
    ...layers_,
    ...temp_layers,
  };

  const joinedDatasetColumns = layers.joins?.condition?.datasetId
    ? await loadColumns(layers.joins.condition.datasetId)
    : undefined;

  const layerVisibilityTable = {
    joins:
      layers.joins &&
      Object.keys(layers.joins).length > 0 &&
      layerToggles.joins === true,
  };

  const applyIfVisible = (layerKey, onTrue) =>
    R.ifElse(() => layerVisibilityTable[layerKey], onTrue, R.identity);

  const newColumns = R.pipe(
    applyIfVisible(
      'joins',
      applyJoinToColumns(joinedDatasetColumns.columns, layers.joins),
    ),
  )(columns);

  const baseState = {
    layers,
    layerToggles,
    columns: newColumns,
    rows: queryResponseToBoardDataRows(
      await redshift.query(
        makeQueryFromLayers(applyIfVisible, datasetId, { ...columns, layers }),
      ),
      newColumns,
    ),
  };
  // todo apply layers to column data

  return baseState;

  // const pg = await makeRedshift();

  // const boardData = await queryJoinedDataset(datasetId, {
  //   ...baseState,
  //   layers: baseState?.layers ?? initial_layers,
  // });

  // const { layers } = boardData;

  // await pg.query(createTableFromColumns(`${datasetId}_working`, boardData.columns));

  // const smartColumnsAndFiltersApplied = R.pipe(
  //   applySmartColumns(layers.smartColumns),
  //   applyFilters(layers.filters),
  // )(boardData);

  // if (
  //   smartColumnsAndFiltersApplied.columns.filter(
  //     col => col.isJoined || col.isSmartColumn,
  //   ).length > 0
  // ) {
  //   await pg.query(knex.schema.dropTable(`${datasetId}_working`).toString());
  //   await pg.query(
  //     createTableFromColumns(`${datasetId}_working`, boardData.columns),
  //   );
  // }

  // const workingBoardData = R.assoc(
  //   'rows',
  //   queryResponseToBoardDataRows(
  //     await pg.query(
  //       makeWorkingTableInsertQuery(datasetId)(smartColumnsAndFiltersApplied),
  //     ),
  //     boardData,
  //   ),
  //   boardData,
  // );

  // const aggregateQuery = buildAggregateLayersQuery(
  //   `${datasetId}_working`,
  //   workingBoardData,
  // );

  // return R.ifElse(
  //   () =>
  //     workingBoardData.layers.groupings?.groupedBy?.length &&
  //     workingBoardData.layerToggles?.groupings,
  //   reconstructGroupedBoardData(workingBoardData.layers.groupings),
  //   R.identity,
  // )({
  //   ...workingBoardData,
  //   rows: queryResponseToBoardDataRows(await pg.query(aggregateQuery), workingBoardData),
  // });
};

module.exports = loadCompiledDataset;
