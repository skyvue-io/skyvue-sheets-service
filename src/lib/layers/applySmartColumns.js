const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const findCellValueByCoordinates = require('../findCellValueByCoordinates');
const findColumnIndexById = require('../findColumnIndexById');
const SyntaxError = require('../../errors/SyntaxError');
const evaluateExpression = require('../evaluateExpression');

const UUID_REGEX = /\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b/g;
// const SPLIT_ON_COMMAS_WITH_WHITESPACE = /,\s*/;
// const EXTRACT_FROM_PARENS_REGEX = /\(([^)]+)\)/;
// const EXTRACT_FROM_QUOTES_REGEX = /"([^']+)"/;

const replaceExpWithValues = (expression, rowIndex, boardData) =>
  R.pipe(
    R.replace(UUID_REGEX, x =>
      findCellValueByCoordinates(
        [rowIndex, findColumnIndexById(x, boardData)],
        boardData,
      ),
    ),
    R.replace(/ = /g, '==='),
  )(expression);

const handleParsingExpression = (expression, rowIndex, col, boardData) => {
  try {
    const filledVariables = replaceExpWithValues(expression, rowIndex, boardData);
    return evaluateExpression(filledVariables);
  } catch (e) {
    console.log(e);
    throw new SyntaxError(col._id);
  }
};

const mapIndexed = R.addIndex(R.map);

const applySmartColumns = R.curry((layers, boardData) => {
  if (!boardData.layerToggles?.smartColumns || layers.length === 0) return boardData;
  try {
    const appendSmartColumnToDataset = R.curry((smartColumn, boardData) => ({
      ...boardData,
      columns: [
        ...boardData.columns,
        {
          ...smartColumn,
          isSmartColumn: true,
          dataType: smartColumn.dataType ?? 'number',
        },
      ],
      rows: mapIndexed((row, index) => ({
        ...row,
        cells: [
          ...row.cells,
          {
            _id: uuidv4(),
            columnId: smartColumn._id,
            value: handleParsingExpression(
              smartColumn.expression,
              index,
              smartColumn,
              boardData,
            ),
          },
        ],
      }))(boardData.rows),
    }));

    const incrementallyAddSmartColumns = R.apply(
      R.pipe,
      R.map(appendSmartColumnToDataset, layers),
    );

    return incrementallyAddSmartColumns(boardData);
  } catch (e) {
    console.log('applySmartColumns:', e);
    return {
      ...boardData,
      errors: [
        ...(boardData.errors ?? []),
        {
          section: 'smartColumns',
          message: e.message,
          type: e.type,
          target: e._id,
        },
      ],
    };
  }
});

module.exports = applySmartColumns;
