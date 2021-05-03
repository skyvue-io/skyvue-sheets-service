const s3 = require('./aws');

const loadDatasetHead = async datasetId => {
  try {
    const s3Params = {
      Bucket: 'skyvue-datasets',
      Key: datasetId,
    };
    const res = await s3.headObject(s3Params).promise();
    const data = JSON.parse(res.Body.toString('utf-8'));

    return data;
  } catch (e) {
    console.error('error in loadDataset, datasetId: ', datasetId);
  }
};

module.exports = loadDatasetHead;
