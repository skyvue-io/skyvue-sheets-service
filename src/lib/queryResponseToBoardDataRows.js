const R = require('ramda');

const mapIndexed = R.addIndex(R.map);

const sortKeysByColumn = columnIds =>
  R.sortBy(R.pipe(R.prop('id'), R.indexOf(R.__, columnIds)));

const queryResponseToBoardDataRows = (data, columns) =>
  R.pipe(
    R.prop('rows'),
    mapIndexed((row, index) => ({
      _id: row.id,
      index,
      cells: R.pipe(
        R.keys,
        R.filter(x => x !== 'id'),
        x => {
          console.log(columns);
          return x;
        },
        sortKeysByColumn(R.pluck('_id', columns)),
        mapIndexed((key, index) => ({
          _id: `${row.id}-${columns[index]?._id ?? ''}`,
          columnId: columns[index]?._id,
          value: row[key] ?? '',
        })),
      )(row),
    })),
  )(data);

module.exports = queryResponseToBoardDataRows;
