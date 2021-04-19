const R = require('ramda');

const makePostgres = require('./postgres');

const queryJoinedDataset = require('./queryJoinedDataset');

const knex = require('../utils/knex');
const createTableFromColumns = require('../lib/queries/createTableFromColumns');
const applySmartColumns = require('../lib/layers/applySmartColumns');
const buildAggregateLayersQuery = require('../lib/layers/buildAggregateLayersQuery');
const applyFilters = require('../lib/layers/applyFilters');
const makeWorkingTableInsertQuery = require('../lib/queries/makeWorkingTableInsertQuery');
const pgQueryToBoardDataRows = require('../lib/pgQueryToBoardDataRows');
const reconstructGroupedBoardData = require('../lib/reconstructGroupedBoardData');

const joinLayer = {
  joinType: 'left',
  condition: {
    datasetId: '607c620690fe20b871ae7083',
    select: ['dcdb77b3-bf37-4137-b63a-56118428be03'],
    on: [
      {
        mainColumnId: 'bf9a6095-1b92-4149-8404-ce20af6ecf50',
        joinedColumnId: '52b45770-329e-423b-8575-4202969234af',
      },
    ],
  },
};

const filterLayer = [
  'AND',
  {
    filterId: 'adsf',
    key: 'b5a5ab33-51e4-4001-b0e5-b19d7de27f22',
    value: 'Roomba',
    predicateType: 'equals',
  },
];

const groupLayer = {
  groupedBy: ['d0b12ecc-5385-4681-9543-70e7b15dcb0d'],
  columnAggregates: {
    'c491b7de-61a0-4b0e-abd0-e12f08eb36f8': 'count',
  },
};

const sortingLayer = [
  { key: 'b5a5ab33-51e4-4001-b0e5-b19d7de27f22', direction: 'asc' },
];

const initial_layers = {
  joins: joinLayer, // {}
  filters: [], // filterLayer, // []
  groupings: {}, // groupLayer, // {}
  smartColumns: [],
  sortings: sortingLayer, // []
  formatting: [],
};
// assumes data is already in postgres
const compileDataset = async (datasetId, baseState) => {
  const pg = await makePostgres();

  const boardData = await queryJoinedDataset(datasetId, {
    ...baseState,
    layers: baseState?.layers ?? initial_layers,
  });

  const { layers } = boardData;

  await pg.query(createTableFromColumns(`${datasetId}_working`, boardData.columns));

  const smartColumnsAndFiltersApplied = R.pipe(
    applySmartColumns(layers.smartColumns),
    applyFilters(layers.filters),
  )(boardData);

  if (smartColumnsAndFiltersApplied.columns.length !== boardData.columns.length) {
    await pg.query(knex.schema.dropTable(`${datasetId}_working`));
    await pg.query(
      createTableFromColumns(`${datasetId}_working`, boardData.columns),
    );
  }

  const workingBoardData = R.assoc(
    'rows',
    pgQueryToBoardDataRows(
      await pg.query(
        makeWorkingTableInsertQuery(datasetId)(smartColumnsAndFiltersApplied),
      ),
      boardData,
    ),
    boardData,
  );

  const aggregateQuery = buildAggregateLayersQuery(
    `${datasetId}_working`,
    workingBoardData,
  );

  return R.ifElse(
    () =>
      workingBoardData.layers.groupings?.groupedBy?.length &&
      workingBoardData.layerToggles?.groupings,
    reconstructGroupedBoardData(workingBoardData.layers.groupings),
    R.identity,
  )({
    ...workingBoardData,
    rows: pgQueryToBoardDataRows(await pg.query(aggregateQuery), workingBoardData),
  });
};

module.exports = compileDataset;
