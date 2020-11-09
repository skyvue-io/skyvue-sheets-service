const path = require('path');

const { parsed } = require('dotenv').config({
  path: path.join(__dirname, '../.env'),
});

module.exports = parsed;
