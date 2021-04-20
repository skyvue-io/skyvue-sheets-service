const aws = require('aws-sdk');
const R = require('ramda');

const applyDatasetLayers = require('../lib/applyDatasetLayers');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');
const { formatValueFromBoardData } = require('../lib/formatValue');
const makeBoardDataFromVersion = require('../lib/makeBoardDataFromVersion');
const importToBaseState = require('../lib/importToBaseState');

const loadDataset = require('../services/loadDataset');
const skyvueFetch = require('../services/skyvueFetch');
const loadS3ToPostgres = require('../services/loadS3ToPostgres');

const addDiff = require('../utils/addDiff');
const addLayer = require('../utils/addLayer');
const parseJson = require('../utils/parseJson');
const stringifyJson = require('../utils/stringifyJson');

// todo move back to s3 from DO spaces
const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const awsConfig = new aws.Config({
  endpoint: spacesEndpoint,
  accessKeyId: process.env.SPACES_KEY,
  secretAccessKey: process.env.SPACES_SECRET,
});

const s3 = new aws.S3(awsConfig);

/**
 * @param {{
 *   columns: IColumn[];
 *   rows: IRow[]
 *   visibilitySettings: {
 *     owner: UserId;
 *     editors: UserId[];
 *     viewers: UserId[];
 *   }
 *  layers: {
 *    joins: [],
 *    filters: [],
 *    groupings: {},
 *    sort: [],
 *    formatting: [],
 *  }
 * }} boardData
 */

// {
//   joinType: 'left',
//   condition: {
//     datasetId: '601c7f25e6c34ab01dc5f726',
//     select: ['22203267-470e-4654-92f1-22d3446c9104'],
//     on: [
//       {
//         mainColumnId: '757909e0-3045-44cb-87d9-853c2595cfbc',
//         joinedColumnId: 'd936f989-82fe-49c1-9483-2eacfe9c43bd',
//         as: 'Owner name',
//       },
//     ],
//   },
// },

const initial_layers = {
  joins: {},
  filters: [],
  groupings: {},
  smartColumns: [],
  sortings: [],
  formatting: [],
};

const Dataset = ({ datasetId, userId }) => {
  // todo build authentication that validates a userId has edit privileges
  const s3Params = {
    Bucket: 'skyvue-datasets',
    Key: datasetId,
  };

  let head;
  let baseState;
  let fnQueue;
  let lastSlice;
  let layers = initial_layers;
  let layerSnapshot = layers;
  let lastCompiledVersion;
  let changeHistory = [];
  let lastAppend;
  // The archive for removed columns
  const removedColumns = {};
  // The cache for compiled boardData objects for each boardId that is joined
  const joinedDatasetCache = {};

  const getCompiled = async (layers, baseState, { saveCompilation = true } = {}) => {
    // todo we can delete literally all of this...
    if (
      R.keys(layers.joins).length > 0 &&
      layers.joins.condition?.datasetId !== datasetId &&
      layers.joins.condition.datasetId &&
      !joinedDatasetCache[layers.joins.condition.datasetId]
    ) {
      const joinedDataset = await loadDataset(layers.joins.condition.datasetId);
      console.log('making the join cache');
      joinedDatasetCache[layers.joins.condition.datasetId] = applyDatasetLayers(
        layers.joins.condition.datasetId,
        joinedDataset?.layers ?? initial_layers,
        {},
        joinedDataset,
      );
    }

    // todo use compileDataset. We may not even need this getCompiled function in general tbh.
    const compiled = applyDatasetLayers(
      datasetId,
      layers,
      joinedDatasetCache,
      baseState,
    );

    if (saveCompilation) {
      lastCompiledVersion = compiled;
    }

    return compiled;
  };

  return {
    get baseState() {
      return baseState;
    },
    get head() {
      return head;
    },
    get meta() {
      return {
        rows: baseState?.rows.length,
      };
    },
    get layers() {
      return layers;
    },
    get lastAppend() {
      return lastAppend;
    },
    get lastSlice() {
      return lastSlice;
    },
    setLastAppend: data => {
      // todo offload this to postgres
      lastAppend = data;
    },
    /**
     * @param {{ columnMapping, dedupeSettings }} importSettings
     */
    importLastAppended: async ({ columnMapping, dedupeSettings }) => {
      if (!lastAppend) return;
      // todo will need to audit this
      // save initial baseState size
      // apply to base state
      // save difference
      // save import log to api server
      const initialBaseStateLength = baseState?.rows?.length ?? 0;
      baseState = importToBaseState({
        columnMapping,
        dedupeSettings,
        importData: lastAppend,
        baseState,
      });
      try {
        await skyvueFetch.post('/datasets/append/log', {
          userId,
          datasetId,
          beginningRowCount: initialBaseStateLength,
          endingRowCount: baseState?.rows?.length ?? 0,
        });
      } catch (e) {
        console.error(e);
      }
    },
    estCSVSize: async () => {
      // todo can we estimate csv size in a postgres query?
      if (!baseState || !lastCompiledVersion) return;
      return R.pipe(boardDataToCSVReadableJSON, jsonToCSV, csv =>
        Buffer.byteLength(csv, 'uft8'),
      )(lastCompiledVersion);
    },
    addLayer: (layerKey, layer) => {
      // todo interaction with snapshots?
      layers = addLayer(layerKey, layer, layers);
      layerSnapshot = layers;
    },
    syncLayers: newLayers => {
      if (R.equals(layers, newLayers)) return;
      layers = newLayers;
    },
    saveToHistory: change => {
      // todo rewrite change history overall. It's pretty garbage right now.
      changeHistory = [...changeHistory, change];
    },
    clearLayers: () => {
      layers = initial_layers;
    },
    toggleLayer: async (toggleKey, isVisible) => {
      baseState = {
        ...baseState,
        layerToggles: {
          ...baseState?.layerToggles,
          [toggleKey]: isVisible,
        },
      };

      return baseState;
    },
    runQueuedFunc: () => {
      fnQueue?.();
    },
    queueFunc: fn => {
      fnQueue = fn;
    },
    clearFuncQueue: () => {
      fnQueue = undefined;
    },
    setLastSlice: (start, end) => {
      // todo I don't think we need this anymore?
      lastSlice = [start, end];
    },
    load: async () => {
      console.log('attempting to load...', s3Params);
      try {
        /*
        TODO:
          - set pipelineIsInitialized = true
          - early return out if pipelineIsInitialized
          - How can we get a super quick check to verify that the base table exists and is healthy?
        */
        const base = await loadS3ToPostgres(datasetId);
        baseState = await compileDataset(datasetId, base.baseState);
        layers = baseState.layers ?? initial_layers;
      } catch (e) {
        console.log('error loading dataset from s3', s3Params, e);
      }

      return baseState;
    },
    // todo write unload function to unload back to s3 and clean up tables on disconnect
    // todo write saveColumnsToS3 to...Well, saveColumnsToS3
    getSlice: async (start, end, { useCached = false } = {}) => {
      /* todo FUTURE APPROACH PSEUDO CODE
      
      get a list of layers that have changed since the last compilation. compileDataset will account for this.
      arrayOfLayersToRedo = diff(layers, layerSnapshot);
      
      Cache check should:
        - Check if rows selected are contained in the in-memory data. If so, return that slice.
        - If not, compileDataset({ future params that will allow us to set row indeces, using sql offset });
      */
      if (useCached && lastCompiledVersion) {
        return {
          ...lastCompiledVersion,
          layers,
          rows: lastCompiledVersion.rows?.slice(start - 1, end) ?? [],
        };
      }

      const compiled = await getCompiled(layers, baseState);

      return {
        ...compiled,
        layers,
        rows: compiled?.rows?.slice(start, end + 1) ?? [],
      };
    },
    // todo make function called getDatasetSummary that returns IDatasetSummary interface from Postgres
    addDiff: async diff => {
      /*
        TODO this will be a big one.

        - If diff is a cell change, asynchronously update directly in Postgres. Client will only care about result if there is an error, in which case it will revert.
        - If diff is a column change, just update the columns in state and call saveColumnsToS3
        - If diff is a column removal, should we just toggle isDeleted? Idk. This will need to be accounted for in change history audits.
        - If diff is a row removal, basically same thing
      */
      const initialBaseState = baseState;
      baseState = addDiff(diff, baseState);

      if (diff.colDiff?.type === 'removal') {
        const removedColumn = diff.colDiff.diff[0];
        if (!removedColumn) return;
        const colIndex = initialBaseState.columns.findIndex(
          col => col._id === removedColumn?._id,
        );
        const cellsInColumn = initialBaseState.rows.map(row => row.cells[colIndex]);
        removedColumns[removedColumn._id] = cellsInColumn;
      }
    },
    exportToCSV: async (title, quantity) => {
      // todo figure literally all of this out
      if (!baseState) return;
      const compiled = await getCompiled(layers, baseState);
      const documents = R.pipe(
        R.assoc(
          'rows',
          R.map(row => ({
            ...row,
            cells: row.cells.map(cell => ({
              ...cell,
              value: formatValueFromBoardData(cell.columnId, cell.value, compiled),
            })),
          }))(compiled.rows),
        ),
        boardDataToCSVReadableJSON,
        x => R.splitEvery(x.length / quantity, x),
      )(compiled);

      const objectUrls = await Promise.all(
        documents.map(async (doc, index) => {
          const fileName = `${datasetId}-${index}`;
          const exportsConfig = new aws.Config({
            region: 'us-west-1',
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
          });
          const exportsS3 = new aws.S3(exportsConfig);
          await exportsS3
            .putObject({
              Bucket: 'skyvue-exported-datasets',
              Key: fileName,
              ContentType: 'text/csv',
              ContentDisposition: `attachment; filename="${title}-${index + 1}.csv`,
              Body: jsonToCSV(doc),
            })
            .promise();

          return process.env.NODE_ENV === 'production'
            ? `https://skyvue-exported-datasets.s3.amazonaws.com/${fileName}`
            : `http://skyvue-exported-datasets.s3.amazonaws.com/${fileName}`;
        }),
      );

      return objectUrls;
    },
    checkoutToVersion: (versionId, direction) => {
      // todo fix change history bc it sucks
      const changeHistoryItem =
        changeHistory.find(history => history.revisionId === versionId) ?? {};

      baseState = makeBoardDataFromVersion(
        changeHistoryItem,
        direction,
        baseState,
        removedColumns,
      );

      return baseState;
    },
    save: async () => {
      // TODO Will we still need this? We may be able to just use saveColumnsToS3, then only persist changes to s3 on unload.

      if (!baseState) return;
      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Body: await stringifyJson({
            ...baseState,
            layers,
          }),
        })
        .promise();
    },
    saveAsNew: async newDatasetId => {
      // todo this will absolutely need to change
      const compiled = await getCompiled(layers, baseState);

      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Key: newDatasetId,
          Body: await stringifyJson({
            ...compiled,
            layers: initial_layers,
          }),
        })
        .promise();

      return true;
    },
  };
};

module.exports = { Dataset, initial_layers };
