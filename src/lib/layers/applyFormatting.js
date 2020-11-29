const R = require('ramda');

const predicateMap = {
  number: value => parseInt(value, 10),
  decimal: value => parseFloat(value),
  percent: value => parseFloat(value) / 100,
  currency: value => `$${parseFloat(value)}`,
};

const applyFormatting = R.curry((layers, boardData) => {
  const columnIdByCellIndex = cellIndex => boardData.columns[cellIndex]?._id;

  const mapIndexed = R.addIndex(R.map);
  const rows = R.map(row => ({
    ...row,
    cells: mapIndexed((cell, index) => {
      const foundLayer = layers.find(
        layer => columnIdByCellIndex(index) === layer.colId,
      );

      return {
        ...cell,
        value: foundLayer
          ? predicateMap[foundLayer.format]?.(cell.value)
          : cell.value,
      };
    })(row.cells),
  }))(boardData.rows);

  return {
    ...boardData,
    rows,
  };
});

module.exports = applyFormatting;
