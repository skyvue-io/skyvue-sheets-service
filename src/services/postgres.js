const { POSTGRES_USER, POSTGRES_HOST, POSTGRES_PASSWORD } = process.env;

const { Client } = require('pg');

const makePostgres = async () => {
  const postgres = new Client({
    user: POSTGRES_USER,
    host: POSTGRES_HOST,
    database: 'postgres',
    password: POSTGRES_PASSWORD,
    port: 5432,
  });

  await postgres.connect();

  return postgres;
};

module.exports = makePostgres;
