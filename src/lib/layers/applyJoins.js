const R = require('ramda');

const applyJoins = R.curry((joinedData, layer, boardData) => {
  if (!boardData.layerToggles?.joins || R.keys(layer).length === 0) return boardData;

  const { joinType, condition } = layer;
  console.log('joinedData', joinedData);
  // todo refactor this when we allow for joining multiple datasets. Until then, just select index[0]
  const { datasetId, on } = condition;

  return boardData;
});

module.exports = applyJoins;
