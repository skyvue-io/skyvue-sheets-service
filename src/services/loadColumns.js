const s3 = require('./aws');

const loadColumns = async datasetId => {
  const res = await s3
    .getObject({
      Bucket: process.env.DATASETS_BUCKET,
      Key: `${datasetId}/columns/0`,
    })
    .promise();

  return JSON.parse(res.Body.toString());
};

module.exports = loadColumns;
