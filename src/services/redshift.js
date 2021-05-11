const { Client } = require('pg');

const makeRedshift = async () => {
  const redshift = new Client();

  await redshift.connect();

  return redshift;
};

module.exports = makeRedshift;
