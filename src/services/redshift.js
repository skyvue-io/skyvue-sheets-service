const { Client } = require('pg');

const makeRedshift = async () => {
  const client = new Client();

  await client.connect();

  return client;
};

module.exports = makeRedshift;
