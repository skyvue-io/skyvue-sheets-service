const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const math = require('mathjs');
const findCellValueByCoordinates = require('../findCellValueByCoordinates');
const findColumnIndexById = require('../findColumnIndexById');

const UUID_REGEX = /\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b/g;

const replaceExpWithValues = (expression, rowIndex, boardData) =>
  expression.replace(UUID_REGEX, x =>
    findCellValueByCoordinates(
      [rowIndex, findColumnIndexById(x, boardData)],
      boardData,
    ),
  );

const handleParsingExpression = (expression, rowIndex, boardData) =>
  math.evaluate(replaceExpWithValues(expression, rowIndex, boardData));

const mapIndexed = R.addIndex(R.map);

const applySmartColumns = R.curry((layers, boardData) => {
  if (boardData.layerToggles.smartColumns === false) return boardData;
  const columns = R.insertAll(
    boardData.columns.length,
    layers.map(layer => {
      const { columnName, _id } = layer;
      return {
        _id,
        dataType: 'number',
        value: columnName,
        isSmartColumn: true,
      };
    }),
    boardData.columns,
  );

  const rows = mapIndexed((row, index) => ({
    ...row,
    cells: [
      ...row.cells,
      ...R.map(layer => ({
        _id: uuidv4(),
        value: handleParsingExpression(layer.expression, index, boardData),
      }))(layers),
    ],
  }))(boardData.rows);

  return {
    ...boardData,
    columns,
    rows,
  };
});

module.exports = applySmartColumns;
