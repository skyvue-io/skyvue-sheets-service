const bigJson = require('big-json');

const parseJson = obj => bigJson.parse({ body: obj });

module.exports = parseJson;
