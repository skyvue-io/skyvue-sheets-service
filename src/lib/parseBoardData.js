const { v4: uuidv4 } = require('uuid');
const R = require('ramda');
const parseDataType = require('./parseDataType');

const parseBoardData = (userId, csvAsJson) => ({
  visibilitySettings: {
    owner: userId,
    editors: [userId],
    viewers: [],
  },
  columns: Object.keys(csvAsJson[0]).map(key => ({
    _id: uuidv4(),
    value: key,
    dataType: parseDataType(csvAsJson[0][key]),
  })),
  rows: csvAsJson.map((row, index) => ({
    _id: uuidv4(),
    index,
    cells: R.pipe(
      R.values,
      R.map(row => ({
        _id: uuidv4(),
        value: row,
      })),
    )(row),
  })),
  layerToggles: {
    groupings: true,
    filters: true,
    joins: true,
    smartColumns: true,
  },
});

module.exports = parseBoardData;
