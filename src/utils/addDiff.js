const addDiff = (diff, baseState) => {
  // todo This should be significantly smarter. It's only set up to handle particular cases.
  const { colDiff, rowDiff } = diff;
  return {
    ...baseState,
    columns: baseState.columns.filter(col => !colDiff.find(d => d._id === col._id)),
    rows: baseState.rows.map(row => rowDiff.find(d => d._id === row._id) ?? row),
  };
};

module.exports = addDiff;
