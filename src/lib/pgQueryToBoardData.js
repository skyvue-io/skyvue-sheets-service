const R = require('ramda');

const mapIndexed = R.addIndex(R.map);

const sortKeysByColumn = columnIds =>
  R.sortBy(R.pipe(R.prop('id'), R.indexOf(R.__, columnIds)));

const pgQueryToBoardData = (data, boardData) =>
  R.pipe(
    R.prop('rows'),
    mapIndexed((row, index) => ({
      _id: row._id,
      index,
      cells: R.pipe(
        R.keys,
        R.filter(x => x !== '_id'),
        sortKeysByColumn(R.pluck('_id', boardData.columns)),
        mapIndexed((key, index) => ({
          _id: `${row._id}-${boardData.columns[index]?._id ?? ''}`,
          columnId: boardData.columns[index]?._id,
          value: row[key] ?? '',
        })),
      )(row),
    })),
  )(data);

module.exports = pgQueryToBoardData;
