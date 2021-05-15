const s3 = require('./aws');

const saveColumnsToS3 = async (datasetId, baseState) =>
  s3
    .putObject({
      Bucket: process.env.DATASETS_BUCKET,
      Key: `${datasetId}/columns/0`,
      ContentType: 'application/json',
      Body: JSON.stringify(baseState),
    })
    .promise();

module.exports = saveColumnsToS3;
