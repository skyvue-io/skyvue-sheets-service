const express = require('express');

const router = express.Router();

router.use('/datasets', (req, res, next) => {
  const { headers } = req;
  if (!headers.secret || headers.secret !== process.env.DATASET_SERVICE_SECRET) {
    return res.sendStatus(401);
  }
  next();
});
router.use('/datasets', require('./datasets'));

module.exports = router;
