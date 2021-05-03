const s3 = require('./aws');
const parseJson = require('../utils/stringifyJson');

const loadDataset = async datasetId => {
  try {
    const s3Params = {
      Bucket: 'skyvue-datasets',
      Key: datasetId,
    };
    const res = await s3.getObject(s3Params).promise();
    const data = await parseJson(res.Body.toString('utf-8'));

    return data;
  } catch (e) {
    console.error('error in loadDataset, datasetId: ', datasetId);
  }
};

module.exports = loadDataset;
