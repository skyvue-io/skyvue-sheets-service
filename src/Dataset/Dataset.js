const aws = require('aws-sdk');
const R = require('ramda');

const applyDatasetLayers = require('../lib/applyDatasetLayers');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');
const { formatValueFromBoardData } = require('../lib/formatValue');
const makeBoardDataFromVersion = require('../lib/makeBoardDataFromVersion');
const importToBaseState = require('../lib/importToBaseState');
const sortDatasetByColumnOrder = require('../lib/sortDatasetByColumnOrder');
const makeSaveRowsQuery = require('../lib/queries/makeSaveRowsQuery');

const skyvueFetch = require('../services/skyvueFetch');
const makeRedshift = require('../services/redshift');
const loadCompiledDataset = require('../services/loadCompiledDataset');
const saveColumnsToS3 = require('../services/saveColumnsToS3');

const addDiff = require('../utils/addDiff');
const addLayer = require('../utils/addLayer');
const parseJson = require('../utils/parseJson');
const stringifyJson = require('../utils/stringifyJson');

const { MAX_IN_MEMORY_ROWS } = require('../constants/boardDataMetaConstants');

const awsConfig = new aws.Config({
  region: 'us-east-2',
  accessKeyId: process.env.AWS_ACCESSKEY,
  secretAccessKey: process.env.AWS_SECRET_ACCESSKEY,
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
    Key: `${datasetId}/columns/0`,
  };

  let head;
  let baseState;
  let fnQueue;
  let lastSlice;
  let layers = initial_layers;
  let layerSnapshot = layers;
  let changeHistory = [];
  let lastAppend;
  let colOrder;
  // The archive for removed columns
  const removedColumns = {};

  return {
    get baseState() {
      return baseState;
    },
    get head() {
      return head;
    },
    get meta() {
      // todo this should query the id column and get the count
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
      // todo offload this to redshift
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
    estCSVSize: async () => 205,
    // todo can we estimate csv size in a redshift query?
    addLayer: (layerKey, layer) => {
      baseState = R.assoc('layers', addLayer(layerKey, layer, layers))(baseState);
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
    addToUnsavedChanges: change => {
      baseState = {
        ...baseState,
        unsavedChanges: {
          ...(baseState?.unsavedChanges ?? {}),
          ...change,
        },
      };
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
    setColOrder: newColOrder => {
      colOrder = newColOrder;
    },
    setDeletedObjects: newDeletedObjects => {
      baseState = {
        ...baseState,
        deletedObjects: newDeletedObjects,
      };
    },
    load: async () => {
      try {
        baseState = await loadCompiledDataset(
          datasetId,
          baseState ? R.omit(['rows'], baseState) : undefined,
        );
        layers = baseState.layers;
        if (!baseState.colOrder && !colOrder) {
          colOrder = R.pluck('_id', baseState.underlyingColumns);
        }
      } catch (e) {
        console.log('error loading dataset from s3', s3Params, e);
      }

      return baseState;
    },
    getSlice: async (start, end, { useCached = false } = {}) => {
      if (end > MAX_IN_MEMORY_ROWS) {
        console.log(
          'This is where I will need to query by offset/row number in Redshift',
        );
        return;
      }

      /* todo FUTURE APPROACH PSEUDO CODE
      
      get a list of layers that have changed since the last compilation. loadCompiledDataset will account for this.
      arrayOfLayersToRedo = diff(layers, layerSnapshot);
      
      Cache check should:
        - Check if rows selected are contained in the in-memory data. If so, return that slice.
        - If not, loadCompiledDataset({ future params that will allow us to set row indeces, using sql offset });
      
      In general, we want to avoid the insert & onConflict & merge calls that we are currently making every time. 
      If we can avoid these by tracking changes, that will pay dividends.
      */

      const returnValue = {
        ...baseState,
        layers,
        rows: baseState?.rows?.slice(start, end + 1) ?? [],
      };
      if (colOrder?.length > 0) {
        return sortDatasetByColumnOrder(colOrder, returnValue);
      }
      return returnValue;
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
      // const initialBaseState = baseState;
      if (diff?.colDiff?.type === 'modification') {
        baseState = addDiff(diff, baseState);
      }

      // if (diff.colDiff?.type === 'removal') {
      //   const removedColumn = diff.colDiff.diff[0];
      //   if (!removedColumn) return;
      //   const colIndex = initialBaseState.columns.findIndex(
      //     col => col._id === removedColumn?._id,
      //   );
      //   const cellsInColumn = initialBaseState.rows.map(row => row.cells[colIndex]);
      //   removedColumns[removedColumn._id] = cellsInColumn;
      // }
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
            ? // todo move this to @tarpleyholdings aws account
              `https://skyvue-exported-datasets.s3.amazonaws.com/${fileName}`
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
    saveRows: async () => {
      const redshift = makeRedshift();
      const query = makeSaveRowsQuery(datasetId, baseState);

      // todo await redshift.query(query);
      // todo
      /*
      baseState = {
        ...baseState,
        unsavedChanges: {},
      }
      */

      return query;
    },
    saveHead: async () => {
      if (!baseState?.columns) return;
      const headToPersist = R.pipe(
        R.assoc('colOrder', colOrder),
        R.omit(['rows', 'underlyingColumns', 'baseColumns']),
        R.assoc('columns', baseState.baseColumns),
      )(baseState);

      // console.log(
      //   'saving something like this',
      //   JSON.stringify(headToPersist, undefined, 2),
      // );
      await saveColumnsToS3(datasetId, headToPersist);
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
