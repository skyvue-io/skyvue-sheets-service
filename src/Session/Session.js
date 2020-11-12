const aws = require('aws-sdk');
const R = require('ramda');
const env = require('../env');
const applyDatasetLayers = require('../lib/applyDatasetLayers');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');

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
 *    groupings: [],
 *    sort: [],
 *    formatting: [],
 *  }
 * }} boardData
 */

const Session = ({ datasetId, userId }) => {
  const s3Params = {
    Bucket: 'skyvue-datasets',
    Key: `${userId}-${datasetId}`,
  };

  let head;
  let baseState;
  let fnQueue;
  let layers = {
    joins: [],
    filters: [],
    groupings: [],
    sortings: [],
    formatting: [],
  };

  const getCompiled = () =>
    baseState && layers ? applyDatasetLayers(layers, baseState) : baseState;

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
    get estCSVSize() {
      if (!baseState) return;
      return R.pipe(boardDataToCSVReadableJSON, jsonToCSV, csv =>
        Buffer.byteLength(csv, 'uft8'),
      )(baseState);
    },
    addLayer: (layerKey, layer) => {
      layers = {
        ...layers,
        [layerKey]: R.uniqWith(
          R.eqProps,
          layers ? [...layers[layerKey], layer] : [layer],
        ),
      };
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
      const res = await s3.getObject(s3Params).promise();
      const data = JSON.parse(res.Body.toString('utf-8'));
      baseState = data;
      layers = baseState.layers;

      return baseState;
    },
    getSlice: (start, end) => {
      const compiled = getCompiled();
      return {
        ...compiled,
        rows:
          compiled?.rows?.filter(row => row.index >= start && row.index <= end) ??
          [],
      };
    },
    addDiff: async diff => {
      const { colDiff, rowDiff } = diff;

      baseState = {
        ...baseState,
        columns: baseState.columns.map(
          col => colDiff.find(d => d._id === col._id) ?? col,
        ),
        rows: baseState.rows.map(row => rowDiff.find(d => d._id === row._id) ?? row),
      };
    },
    inViewState: () => undefined,
    csvFileSize: () => undefined,
    setBaseState: () => undefined,
    save: async () => {
      console.log('saving', layers);
      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Body: JSON.stringify({
            layers,
            ...baseState,
          }),
        })
        .promise();
    },
    saveAsNew: () => undefined,
  };
};

module.exports = Session;
