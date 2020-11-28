const _ = require('lodash');
const R = require('ramda');
const safeParseNumber = require('../../utils/safeParseNumber');

const applySortings = R.curry((sortLayer, boardData) => ({
  ...boardData,
  rows: _.orderBy(
    boardData.rows,
    sortLayer.map(layer => row =>
      safeParseNumber(
        R.prop(
          'value',
          R.path(
            ['cells', R.findIndex(x => x._id === layer.key)(boardData.columns)],
            row,
          ),
        ),
      ),
    ),
    sortLayer.map(x => x.direction),
  ),
}));

module.exports = applySortings;
