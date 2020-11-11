const aws = require('aws-sdk');
const R = require('ramda');
const env = require('../env');
const boardDataToCSVReadableJSON = require('../lib/boardDataToCSVReadableJSON');
const jsonToCSV = require('../lib/jsonToCSV');

const awsConfig = new aws.Config({
  region: 'us-west-1',
  accessKeyId: env.AWS_ACCESS_KEY,
  secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new aws.S3(awsConfig);

const Session = ({ datasetId, userId }) => {
  const s3Params = {
    Bucket: 'skyvue-datasets',
    Key: `${userId}-${datasetId}`,
  };

  let head;
  let baseState;
  let fnQueue;

  return {
    get baseState() {
      return baseState;
    },
    get head() {
      return head;
    },
    get estCSVSize() {
      if (!baseState) return;
      return R.pipe(boardDataToCSVReadableJSON, jsonToCSV, csv =>
        Buffer.byteLength(csv, 'uft8'),
      )(baseState);
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
      return baseState;
    },
    getSlice: (start, end) => ({
      ...baseState,
      rows:
        baseState?.rows?.filter(row => row.index >= start && row.index <= end) ?? [],
    }),
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
      await s3
        .putObject({
          ...s3Params,
          ContentType: 'application/json',
          Body: JSON.stringify(baseState),
        })
        .promise();
    },
    saveAsNew: () => undefined,
    get layers() {
      return 'hi';
    },
  };
};

module.exports = Session;
