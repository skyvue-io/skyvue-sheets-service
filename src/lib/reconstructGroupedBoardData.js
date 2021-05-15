const R = require('ramda');
const { v4: uuid } = require('uuid');
const _ = require('lodash');

const reconstructGroupedBoardData = R.curry((groupLayer, boardData) => {
  const { groupedBy, columnAggregates } = groupLayer;
  const { rows } = boardData;
  const aggregateKeys = Object.keys(columnAggregates);
  const colIdsRepresented = [...groupedBy, ...aggregateKeys];

  const columnsLookup = R.indexBy(R.prop('_id'), boardData.columns);

  const mapIndexed = R.addIndex(R.map);

  const columns = R.map(colId => ({
    _id: colId,
    value: columnAggregates[colId]
      ? `${_.startCase(columnAggregates[colId])} of ${columnsLookup[colId]?.value}`
      : columnsLookup[colId]?.value,
    dataType: columnsLookup[colId]?.dataType ?? 'number',
    isGrouped: true,
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
          columnId: columns[index]?._id,
          _id: `${rowId}-${columns[index]?._id}`,
        }))(row.cells),
      };
    })(rows),
  };
});

module.exports = reconstructGroupedBoardData;
