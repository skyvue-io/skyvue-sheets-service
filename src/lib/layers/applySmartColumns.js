const R = require('ramda');

const predicateMap = {
  sum: R.reduce((a, b) => parseFloat(a) + parseFloat(b), 0),
  divide: R.divide,
  subtract: R.subtract,
  concat: (values, delim) => values.join(delim ?? ''),
  avg: R.mean,
};

const mapDataTypeToPredicate = {
  sum: 'number',
  divide: 'number',
  subtract: 'number',
  concat: 'string',
  avg: 'number',
};

const applySmartColumns = R.curry((sortLayer, boardData) => {
  const columns = R.insertAll(
    boardData.columns.length,
    sortLayer.map(layer => {
      const { predicate, columnName, _id } = layer;
      return {
        _id,
        dataType: mapDataTypeToPredicate[predicate],
        value: columnName,
      };
    }),
    boardData.columns,
  );

  const rows = R.map(row => ({
    ...row,
    cells: [
      ...row.cells,
      ...R.map(layer => {
        const { predicate, columns } = layer;
        const colIndeces = columns.map(col =>
          boardData.columns.findIndex(_col => col === _col._id),
        );
        const vals = colIndeces.map(index => row.cells[index].value);
        return {
          _id: uuidv4(),
          value: predicateMap[predicate](
            ['divide', 'subtract'].includes(predicate) ? (vals[0], vals[1]) : vals,
          ),
        };
      })(sortLayer),
    ],
  }))(boardData.rows);

  return R.pipe(R.assoc('columns', columns), R.assoc('rows', rows))(boardData);
});

module.exports = applySmartColumns;
