const R = require('ramda');

const mapIndexed = R.addIndex(R.map);
const sortKeysByColumnOrder = require('./sortKeysByColumnOrder');

const queryResponseToBoardDataRows = (data, columns) =>
  R.pipe(
    R.prop('rows'),
    mapIndexed((row, index) => ({
      _id: row.id,
      index,
      cells: R.pipe(
        R.keys,
        R.filter(x => x !== 'id'),
        sortKeysByColumnOrder(R.pluck('_id', columns)),
        mapIndexed((key, index) => ({
          _id: `${row.id}-${columns[index]?._id ?? ''}`,
          columnId: columns[index]?._id,
          value: row[key] ?? '',
        })),
      )(row),
    })),
  )(data);

module.exports = queryResponseToBoardDataRows;
