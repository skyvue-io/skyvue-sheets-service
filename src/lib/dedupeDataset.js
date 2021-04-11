const R = require('ramda');

const dedupeDataset = R.curry((dedupeSettings, boardData) => {
  if (!dedupeSettings || R.keys(dedupeSettings).length === 0) return boardData;
  const { keep, dedupeOn } = dedupeSettings;

  const colIndeces = R.map(colId =>
    boardData.columns.findIndex(col => col._id === colId),
  )(dedupeOn);

  const pluckValues = R.pipe(
    R.prop('cells'),
    R.pick(colIndeces),
    R.values,
    R.pluck('value'),
  );

  const dedupedRows = R.pipe(
    R.ifElse(() => keep === 'last', R.reverse, R.identity),
    R.uniqBy(pluckValues),
  )(boardData.rows);

  return R.assoc('rows', dedupedRows)(boardData);
});

module.exports = dedupeDataset;
