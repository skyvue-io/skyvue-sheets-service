const aws = require('aws-sdk');
const R = require('ramda');

const applyDatasetLayers = require('../lib/applyDatasetLayers');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');
const { formatValueFromBoardData } = require('../lib/formatValue');
const makeBoardDataFromVersion = require('../lib/makeBoardDataFromVersion');

const loadDataset = require('../services/loadDataset');

const addDiff = require('../utils/addDiff');
const addLayer = require('../utils/addLayer');

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
  let lastCompiledVersion;
  let changeHistory = [];
  let lastAppend;
  // The archive for removed columns
  const removedColumns = {};
  // The cache for compiled boardData objects for each boardId that is joined
  const joinedDatasetCache = {};

  const getCompiled = async (layers, baseState, { saveCompilation = true } = {}) => {
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
      lastAppend = data;
    },
    estCSVSize: async () => {
      if (!baseState || !lastCompiledVersion) return;
      return R.pipe(boardDataToCSVReadableJSON, jsonToCSV, csv =>
        Buffer.byteLength(csv, 'uft8'),
      )(lastCompiledVersion);
    },
    addLayer: (layerKey, layer) => {
      layers = addLayer(layerKey, layer, layers);
    },
    syncLayers: newLayers => {
      if (R.equals(layers, newLayers)) return;
      layers = newLayers;
    },
    saveToHistory: change => {
      changeHistory = [...changeHistory, change];
    },
    clearLayers: () => {
      layers = initial_layers;
    },
    toggleLayer: async (toggle, visible) => {
      baseState = {
        ...baseState,
        layerToggles: {
          ...baseState?.layerToggles,
          [toggle]: visible,
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
      lastSlice = [start, end];
    },
    load: async () => {
      console.log('attempting to load...', s3Params);
      try {
        head = await s3.headObject(s3Params).promise();
        const res = await s3.getObject(s3Params).promise();
        const data = JSON.parse(res.Body.toString('utf-8'));
        baseState = data;
        layers = baseState.layers ?? initial_layers;
      } catch (e) {
        console.log('error loading dataset from s3', s3Params, e);
      }

      return baseState;
    },
    getSlice: async (start, end, { useCached = false } = {}) => {
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
        // compiled?.rows?.filter(row => row.index >= start && row.index <= end) ??
        // [],
      };
    },
    addDiff: async diff => {
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
      if (!baseState) return;
      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Body: JSON.stringify({
            ...baseState,
            layers,
          }),
        })
        .promise();
    },
    saveAsNew: async newDatasetId => {
      const compiled = await getCompiled(layers, baseState);

      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Key: newDatasetId,
          Body: JSON.stringify({
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
