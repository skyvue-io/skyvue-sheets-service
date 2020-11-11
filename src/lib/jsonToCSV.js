const { parse } = require('json2csv');

const jsonToCSV = csvReadableJSON => {
  const fields = Object.keys(csvReadableJSON[0]);
  const opts = { fields };

  const csv = parse(csvReadableJSON, opts);
  return csv;
};

module.exports = jsonToCSV;
