const R = require('ramda');

const makePostgres = require('./postgres');

const queryJoinedDataset = require('./queryJoinedDataset');

const knex = require('../utils/knex');
const createTableFromColumns = require('../lib/queries/createTableFromColumns');
const applySmartColumns = require('../lib/layers/applySmartColumns');
const applyFilters = require('../lib/layers/applyFilters');
const makeWorkingTableInsertQuery = require('../lib/queries/makeWorkingTableInsertQuery');
const pgQueryToBoardDataRows = require('../lib/pgQueryToBoardDataRows');

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

const initial_layers = {
  joins: joinLayer,
  filters: [],
  groupings: {},
  smartColumns: [],
  sortings: [],
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

  return R.assoc(
    'rows',
    pgQueryToBoardDataRows(
      await pg.query(
        makeWorkingTableInsertQuery(datasetId)(smartColumnsAndFiltersApplied),
      ),
      boardData,
    ),
    baseState,
  );

  // const aggregateQuery = R.pipe(
  //   applyColSummaries,
  //   applyGrouping,
  //   applySorting,
  //   applyRowIndeces,
  // );

  // return pg.query(knex.select().table(`${datasetId}_working`).toString());
  // return boardData;
};

module.exports = compileDataset;
