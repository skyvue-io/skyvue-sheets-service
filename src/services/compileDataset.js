const R = require('ramda');

const knex = require('../utils/knex');
const makePostgres = require('./postgres');

const queryJoinedDataset = require('./queryJoinedDataset');

const createTableFromColumns = require('../lib/queries/createTableFromColumns');
const applySmartColumns = require('../lib/layers/applySmartColumns');
const applyFilters = require('../lib/layers/applyFilters');
const makeWorkingTableInsertQuery = require('../lib/queries/makeWorkingTableInsertQuery');

const initial_layers = {
  joins: {},
  filters: [],
  groupings: {},
  smartColumns: [],
  sortings: [],
  formatting: [],
};
// assumes data is already in postgres
const compileDataset = async (datasetId, baseState) => {
  const pg = await makePostgres();

  const datasetWithJoins = await queryJoinedDataset(datasetId, baseState);

  const boardData = {
    ...baseState,
    rows: datasetWithJoins,
    layers: baseState?.layers ?? initial_layers,
  };

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

  return pg.query(
    makeWorkingTableInsertQuery(datasetId)(smartColumnsAndFiltersApplied),
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
