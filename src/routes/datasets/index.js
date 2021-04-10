const express = require('express');

const router = express.Router();
const csv = require('csvtojson');

const s3 = require('../../services/aws');
const parseBoardData = require('../../lib/parseBoardData');

router.post('/process_dataset', async (req, res) => {
  const { body } = req;
  const { key, userId } = body;
  if (!key) return res.sendStatus(400);

  try {
    const s3Res = await s3
      .getObject({
        Bucket: 'skyvue-datasets-queue',
        Key: key,
      })
      .promise();
    const csvAsJson = await csv().fromString(s3Res.Body.toString('utf8'));
    const boardData = parseBoardData(userId, csvAsJson);

    const s3Params = {
      Bucket: 'skyvue-datasets',
      Key: key,
      Body: JSON.stringify(boardData),
      ContentType: 'application/json',
    };

    try {
      await s3.putObject(s3Params).promise();
      await s3
        .deleteObject({
          Bucket: 'skyvue-datasets-queue',
          Key: key,
        })
        .promise();
      res.sendStatus(200);
    } catch (e) {
      console.log(e);
      res.sendStatus(400);
    }
  } catch (e) {
    console.error('error in dataset processing, datasetId: ', e);
    return res.sendStatus(204);
  }
});

module.exports = router;
