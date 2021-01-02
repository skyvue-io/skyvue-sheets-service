const handleColumnTimeTravel = (targetId, value, boardData) =>
  value
    ? {
        ...boardData,
        columns: [...boardData.columns, value],
      }
    : {
        ...boardData,
        columns: boardData.columns.filter(col => col._id !== targetId),
      };

module.exports = handleColumnTimeTravel;
