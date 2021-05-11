const R = require('ramda');

const mapIndexed = R.addIndex(R.map);

const sortKeysByColumn = columnIds =>
  R.sortBy(R.pipe(R.prop('id'), R.indexOf(R.__, columnIds)));

const queryResponseToBoardDataRows = (data, boardData) =>
  R.pipe(
    R.prop('rows'),
    x => {
      console.log(x);
      return x;
    },
    mapIndexed((row, index) => ({
      _id: row.id,
      index,
      cells: R.pipe(
        R.keys,
        R.filter(x => x !== 'id'),
        sortKeysByColumn(R.pluck('_id', boardData.columns)),
        mapIndexed((key, index) => ({
          _id: `${row.id}-${boardData.columns[index]?._id ?? ''}`,
          columnId: boardData.columns[index]?._id,
          value: row[key] ?? '',
        })),
      )(row),
    })),
  )(data);

module.exports = queryResponseToBoardDataRows;
