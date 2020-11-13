const R = require('ramda');

const applyDatasetLayers = (layers, boardData) => {
  const predicateMap = {
    // eslint-disable-next-line eqeqeq
    equals: (a, b) => a == b,
    // eslint-disable-next-line eqeqeq
    notEquals: (a, b) => a != b,
    contains: (a, b) => b.includes(a),
    lessThan: (a, b) => a < b,
    lessThanEqualTo: (a, b) => a <= b,
    greaterThan: (a, b) => a > b,
    greaterThanEqualTo: (a, b) => a <= b,
    dateBetween: R.__,
  };

  const layerFunctions = {
    joins: R.identity,
    filters: R.curry((layers, rows) => {
      const applyToLayer = layer => {
        const colAppliedTo = boardData.columns.findIndex(
          col => col._id === layer.colId,
        );

        return R.filter(row =>
          predicateMap[layer.predicateType](
            layer.value,
            row.cells[colAppliedTo].value,
          ),
        )(rows);
      };

      return R.pipe(R.map(applyToLayer), R.flatten)(layers);
    }),
    groupings: R.identity,
    sortings: R.identity,
    formatting: R.identity,
  };

  const applyLayers = R.pipe(
    layerFunctions.joins,
    layerFunctions.filters(layers.filters),
    layerFunctions.groupings,
    layerFunctions.sortings,
    layerFunctions.formatting,
  )(boardData.rows);

  return {
    ...boardData,
    rows: applyLayers,
  };
};

module.exports = applyDatasetLayers;
