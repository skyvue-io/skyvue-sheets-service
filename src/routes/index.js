const express = require('express');

const router = express.Router();
const csv = require('csvtojson');

const makeRedshift = require('../services/redshift');
const s3 = require('../services/aws');

router.use('/datasets', (req, res, next) => {
  const { headers } = req;
  if (!headers.secret || headers.secret !== process.env.DATASET_SERVICE_SECRET) {
    return res.sendStatus(401);
  }
  next();
});
router.use('/datasets', require('./datasets'));

router.get('/test', async (req, res) => {
  const client = await makeRedshift();
  console.log(Object.keys(client), client._queryable, client._connected);
  const result = await client.query('select * from information_schema.tables');
  res.json(result.rows);
});

module.exports = router;
