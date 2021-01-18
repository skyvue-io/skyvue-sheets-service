const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const math = require('mathjs');
const findCellValueByCoordinates = require('../findCellValueByCoordinates');
const findColumnIndexById = require('../findColumnIndexById');

const CONTENTS_IN_PARENS = /\((.*)\)/;

const replaceExpWithValues = (expression, rowIndex, boardData) => {
  const test = expression.split(' ').map(x => {
    const returned =
      x.startsWith('col(') && x.endsWith(')')
        ? x
            .replace(CONTENTS_IN_PARENS, x =>
              findCellValueByCoordinates(
                [rowIndex, findColumnIndexById(x.slice(1, x.length - 1), boardData)],
                boardData,
              ),
            )
            .slice('col'.length)
        : x;

    return returned;
  });

  return test.join('');
};

const handleParsingExpression = (expression, rowIndex, boardData) =>
  math.evaluate(replaceExpWithValues(expression, rowIndex, boardData));

const mapIndexed = R.addIndex(R.map);

const applySmartColumns = R.curry((layers, boardData) => {
  const columns = R.insertAll(
    boardData.columns.length,
    layers.map(layer => {
      const { columnName } = layer;
      return {
        _id: uuidv4(),
        dataType: 'number',
        value: columnName,
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
