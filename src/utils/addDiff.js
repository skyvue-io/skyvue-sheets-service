const addDiff = (diff, baseState) => {
  const { colDiff, rowDiff } = diff;

  return {
    ...baseState,
    columns: baseState.columns.map(
      col => colDiff.find(d => d._id === col._id) ?? col,
    ),
    rows: baseState.rows.map(row => rowDiff.find(d => d._id === row._id) ?? row),
  };
};

module.exports = addDiff;
