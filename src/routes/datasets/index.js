const express = require('express');
const R = require('ramda');

const router = express.Router();
const csv = require('csvtojson');
const pRetry = require('p-retry');

const skyvueFetch = require('../../services/skyvueFetch');
const loadCompiledDataset = require('../../services/loadCompiledDataset');
const loadS3ToPostgres = require('../../services/loadS3ToPostgres');
const s3 = require('../../services/aws');
const makeRedshift = require('../../services/redshift');
const parseBoardData = require('../../lib/parseBoardData');

const stringifyJson = require('../../utils/stringifyJson');

router.post('/test_load', async (req, res) => {
  const { key } = req.body;
  const base = await loadS3ToPostgres(key);
  const baseState = await loadCompiledDataset(key, base.baseState);
  res.json(baseState);
});

router.get('/test_get', async (req, res) => {
  const pg = await makeRedshift();
  const response = await pg.query('select * from test');

  res.send(response);
});

router.post('/process_dataset', async (req, res) => {
  const { body } = req;
  const { key, userId } = body;

  if (!key) return res.sendStatus(400);

  try {
    console.log('loading from s3', key);
    res.sendStatus(200);
    const s3Res = await s3
      .getObject({
        Bucket: 'skyvue-datasets-queue',
        Key: key,
      })
      .promise();
    const csvAsJson = await csv().fromString(s3Res.Body.toString('utf8'));

    const boardData = parseBoardData(userId, csvAsJson);

    try {
      await s3
        .putObject({
          Bucket: 'skyvue-datasets',
          Key: `${key}/columns`,
          Body: await stringifyJson(R.omit(['rows'], boardData)),
          ContentType: 'application/json',
        })
        .promise();
    } catch (e) {
      console.error('problem uploading columns', e);
    }

    R.splitEvery(50000, boardData.rows).map(async (item, index) => {
      const s3Params = {
        Bucket: 'skyvue-datasets',
        Key: `${key}/rows/${index}`,
        Body: await stringifyJson(item),
        ContentType: 'application/json',
      };

      await pRetry(async () => s3.putObject(s3Params).promise(), { retries: 5 });
    });

    try {
      console.log('deleting from queue');
      await s3
        .deleteObject({
          Bucket: 'skyvue-datasets-queue',
          Key: key,
        })
        .promise();

      console.log('notifying db that request is complete');
      await skyvueFetch.patch(`/datasets/${key}`, { isProcessing: false });
    } catch (e) {
      console.log(e);
      res.sendStatus(400);
    }
  } catch (e) {
    console.error('error in dataset processing, datasetId: ', e);
  }
});

module.exports = router;
