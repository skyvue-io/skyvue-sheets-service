const aws = require('aws-sdk');
const R = require('ramda');
const env = require('../env');

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

  const idleSaveTimer = null;
  let head;
  let baseState;

  const save = async () => {
    await s3
      .putObject({
        ...s3Params,
        ContentType: 'application/json',
        Body: JSON.stringify(baseState),
      })
      .promise();
  };

  return {
    get baseState() {
      return baseState;
    },
    get head() {
      return head;
    },
    load: async () => {
      head = await s3.headObject(s3Params).promise();
      const res = await s3.getObject(s3Params).promise();
      const data = JSON.parse(res.Body.toString('utf-8'));
      baseState = data;
      return baseState;
    },
    addChange: async diff => {
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
    save,
    saveAsNew: () => undefined,
    get layers() {
      return 'hi';
    },
  };
};

module.exports = Session;
