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

const initial_layers = {
  joins: {}, // {}
  filters: [], // filterLayer, // []
  groupings: {}, // groupLayer, // {}
  smartColumns: [],
  sortings: [], // []
  formatting: [],
};

// assumes data is already in postgres
const loadCompiledDataset = async (datasetId, columnsAndLayers) => {
  const redshift = await makeRedshift();
  const columns = columnsAndLayers ?? (await loadColumns(datasetId));
  const layers = columns.layers ?? initial_layers;

  const baseState = {
    ...columns,
    layers,
    rows: queryResponseToBoardDataRows(
      await redshift.query(makeQueryFromLayers(datasetId, { ...columns, layers })),
      columns,
    ),
  };

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
