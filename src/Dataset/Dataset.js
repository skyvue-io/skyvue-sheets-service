const aws = require('aws-sdk');
const R = require('ramda');
const env = require('../env');
const applyDatasetLayers = require('../lib/applyDatasetLayers');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');
const handleColumnTimeTravel = require('../lib/handleColumnTimeTravel');
const makeBoardDataFromVersion = require('../lib/makeBoardDataFromVersion');

const addDiff = require('../utils/addDiff');
const addLayer = require('../utils/addLayer');
const updateCellById = require('../utils/updateCellById');
const updateColumnById = require('../utils/updateColumnById');

const awsConfig = new aws.Config({
  region: 'us-west-1',
  accessKeyId: env.AWS_ACCESS_KEY,
  secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
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
  joins: [],
  filters: [],
  groupings: {},
  smartColumns: [],
  sortings: [],
  formatting: [],
};

const getCompiled = (layers, baseState) =>
  baseState // && !R.whereEq(layers)(initial_layers)
    ? applyDatasetLayers(layers, baseState)
    : baseState;

const Dataset = ({ datasetId, userId }) => {
  const s3Params = {
    Bucket: 'skyvue-datasets',
    Key: `${userId}-${datasetId}`,
  };

  let head;
  let baseState;
  let fnQueue;
  let layers = initial_layers;
  let changeHistory = [];

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
    get estCSVSize() {
      if (!baseState) return;
      return R.pipe(boardDataToCSVReadableJSON, jsonToCSV, csv =>
        Buffer.byteLength(csv, 'uft8'),
      )(baseState);
    },
    addLayer: (layerKey, layer) => {
      layers = addLayer(layerKey, layer, layers);
    },
    saveToHistory: change => {
      changeHistory = [...changeHistory, change];
    },
    clearLayers: () => {
      layers = initial_layers;
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
    load: async () => {
      head = await s3.headObject(s3Params).promise();
      console.log(s3Params);

      const res = await s3.getObject(s3Params).promise();
      const data = JSON.parse(res.Body.toString('utf-8'));
      baseState = data;
      layers = baseState.layers ?? initial_layers;

      return baseState;
    },
    getSlice: (start, end) => {
      const compiled = getCompiled(layers, baseState);

      return {
        ...compiled,
        layers,
        rows:
          compiled?.rows?.filter(row => row.index >= start && row.index <= end) ??
          [],
      };
    },
    addDiff: async diff => {
      baseState = addDiff(diff, baseState);
    },
    exportToCSV: async (title, quantity) => {
      if (!baseState) return;
      const compiled = getCompiled(layers, baseState);
      const documents = R.pipe(boardDataToCSVReadableJSON, x =>
        R.splitEvery(x.length / quantity, x),
      )(compiled);

      const objectUrls = await Promise.all(
        documents.map(async (doc, index) => {
          const fileName = `${userId}-${datasetId}-${index}`;
          await s3
            .putObject({
              Bucket: 'skyvue-exported-datasets',
              Key: fileName,
              ContentType: 'text/csv',
              ContentDisposition: `attachment; filename="${title}-${index + 1}.csv`,
              Body: jsonToCSV(doc),
            })
            .promise();

          return `http://skyvue-exported-datasets.s3.amazonaws.com/${fileName}`;
        }),
      );

      return objectUrls;
    },
    checkoutToVersion: (versionId, direction) => {
      const { targetId, changeTarget, prevValue, newValue } =
        changeHistory.find(history => history.revisionId === versionId) ?? {};

      const changeHistoryItem =
        changeHistory.find(history => history.revisionId === versionId) ?? {};

      const temp = makeBoardDataFromVersion(changeHistoryItem, direction, baseState);
      console.log(temp);
      const updateFunc =
        changeTarget === 'cell'
          ? updateCellById
          : changeTarget === 'column'
          ? typeof newValue === 'object' || typeof prevValue === 'object'
            ? handleColumnTimeTravel
            : updateColumnById
          : (x, y, z) => baseState;

      baseState = updateFunc(
        targetId,
        direction === 'undo' ? prevValue : newValue,
        baseState,
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
      const compiled = getCompiled(layers, baseState);

      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Key: `${userId}-${newDatasetId}`,
          Body: JSON.stringify({
            ...compiled,
            layers: initial_layers,
          }),
        })
        .promise();

      return true;
    },
    inViewState: () => undefined,
    csvFileSize: () => undefined,
    setBaseState: () => undefined,
  };
};

module.exports = Dataset;
