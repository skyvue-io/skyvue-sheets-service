const R = require('ramda');
const { v4: uuidv4 } = require('uuid');
const math = require('mathjs');
const findCellValueByCoordinates = require('../findCellValueByCoordinates');
const findColumnIndexById = require('../findColumnIndexById');
const SyntaxError = require('../../errors/SyntaxError');

const UUID_REGEX = /\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b/g;
const SPLIT_ON_COMMAS_WITH_WHITESPACE = /,\s*/;
const EXTRACT_FROM_PARENS_REGEX = /\(([^)]+)\)/;
const EXTRACT_FROM_QUOTES_REGEX = /"([^']+)"/;

const extractFromConcatString = R.pipe(
  R.match(EXTRACT_FROM_PARENS_REGEX),
  R.prop(1),
  R.split(SPLIT_ON_COMMAS_WITH_WHITESPACE),
  R.map(word =>
    word.charAt(0) === '"' && word.charAt(word.length - 1) === '"'
      ? word
      : `"${word}"`,
  ),
  R.map(word => (word === '" "' ? ' ' : word.match(EXTRACT_FROM_QUOTES_REGEX)?.[1])),
  R.join(''),
);

const replaceExpWithValues = (expression, rowIndex, boardData) =>
  expression.replace(UUID_REGEX, x =>
    findCellValueByCoordinates(
      [rowIndex, findColumnIndexById(x, boardData)],
      boardData,
    ),
  );

const handleParsingExpression = (expression, rowIndex, col, boardData) => {
  try {
    const filledVariables = replaceExpWithValues(expression, rowIndex, boardData);
    if (col.dataType === 'string') {
      return extractFromConcatString(filledVariables);
    }
    return math.evaluate(filledVariables);
  } catch (e) {
    throw new SyntaxError(col._id);
  }
};

const mapIndexed = R.addIndex(R.map);

const applySmartColumns = R.curry((layers, boardData) => {
  if (boardData.layerToggles.smartColumns === false) return boardData;
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

// const applySmartColumns = R.curry((layers, boardData) => {
//   if (boardData.layerToggles.smartColumns === false) return boardData;
//   try {
//     const columns = R.insertAll(
//       boardData.columns.length,
//       layers.map(layer => ({
//         ...layer,
//         dataType: layer.dataType ?? 'number',
//         isSmartColumn: true,
//       })),
//       boardData.columns,
//     );

//     const rows = mapIndexed((row, index) => ({
//       ...row,
//       cells: [
//         ...row.cells,
//         ...R.map(layer => ({
//           _id: uuidv4(),
//           value: handleParsingExpression(layer.expression, index, layer, boardData),
//         }))(layers),
//       ],
//     }))(boardData.rows);

//     return {
//       ...boardData,
//       columns,
//       rows,
//     };
//   } catch (e) {
//     return {
//       ...boardData,
//       errors: [
//         ...(boardData.errors ?? []),
//         {
//           section: 'smartColumns',
//           message: e.message,
//           type: e.type,
//           target: e._id,
//         },
//       ],
//     };
//   }
// });

module.exports = applySmartColumns;
