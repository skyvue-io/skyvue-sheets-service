const fetch = require('node-fetch');

const makeFetch = method => (route, body) =>
  fetch(`${process.env.SKYVUE_API_URL}${route}`, {
    method,
    body: body ? JSON.stringify(body) : undefined,
    headers: {
      'Content-Type': 'application/json',
      secret: process.env.DATASET_SERVICE_SECRET,
    },
  });

const skyvueFetch = {
  post: makeFetch('POST'),
};

module.exports = skyvueFetch;
