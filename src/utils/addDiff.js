const R = require('ramda');

const addDiff = (diff, baseState) => {
  if (!baseState) return;
  /**
   * diff: {
   *  diff: IRow[] | IColumn [];
   *  type: 'modification' | 'addition' | 'removal';
   * }
   */
  const { colDiff, rowDiff } = diff;

  const filterOrMap = (diffType, iterable, diff) =>
    diffType === 'modification'
      ? R.map(x => diff.find(d => d._id === x._id) ?? x)(iterable)
      : diffType === 'removal'
      ? R.filter(x => !diff.find(d => d._id === x._id))(iterable)
      : iterable;

  const { baseColumns, columns, rows } = baseState;
  return {
    ...baseState,
    baseColumns: colDiff
      ? filterOrMap(colDiff.type, baseColumns, colDiff.diff)
      : baseColumns,
    columns: colDiff ? filterOrMap(colDiff.type, columns, colDiff.diff) : columns,
    rows: rowDiff ? filterOrMap(rowDiff.type, rows, rowDiff.diff) : rows,
  };
};

module.exports = addDiff;
