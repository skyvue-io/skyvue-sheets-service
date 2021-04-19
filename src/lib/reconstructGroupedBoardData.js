const R = require('ramda');
const { v4: uuid } = require('uuid');

const reconstructGroupedBoardData = R.curry((groupLayer, boardData) => {
  const { groupedBy, columnAggregates } = groupLayer;
  const aggregateKeys = Object.keys(columnAggregates);
  const colIdsRepresented = [...groupedBy, ...aggregateKeys];

  const mapIndexed = R.addIndex(R.map);

  const columns = R.map(colId => ({
    _id: colId,
    dataType: 'string', // todo change when you have better type detection
  }))(colIdsRepresented);

  return {
    ...boardData,
    columns,
    rows: mapIndexed((row, index) => {
      const rowId = uuid();
      return {
        _id: rowId,
        index,
        cells: mapIndexed((cell, index) => ({
          ...cell,
          _id: `${rowId}-${columns[index]?._id}`,
        }))(row.cells),
      };
    })(boardData.rows),
  };
});

module.exports = reconstructGroupedBoardData;
