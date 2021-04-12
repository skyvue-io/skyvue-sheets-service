const bigJson = require('big-json');

const stringifyJson = obj => bigJson.stringify({ body: obj });

module.exports = stringifyJson;
