const findColumnById = (colId, boardData) =>
  boardData.columns.find(col => col._id === colId);

module.exports = findColumnById;
