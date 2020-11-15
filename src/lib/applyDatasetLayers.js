const R = require('ramda');
const applyFilters = require('./layers/applyFilters');

const applyDatasetLayers = (layers, boardData) => {
  const layerFunctions = {
    joins: R.identity,
    filters: applyFilters,
    groupings: R.identity,
    sortings: R.identity,
    formatting: R.identity,
  };

  const conditions = [
    'AND',
    {
      key: '2d9dc775-d7f2-4acc-ba07-6f20493abac8',
      value: '1',
      predicateType: 'greaterThanEqualTo',
    },
    // [
    //   'OR',
    //   {
    //     key: '2d9dc775-d7f2-4acc-ba07-6f20493abac8',
    //     value: '1',
    //     predicateType: 'equals',
    //   },
    //   {
    //     key: 'fc8d530e-41d2-43f0-87ce-9e30ab6f8c06',
    //     value: 'marvel/0001/000.jpg',
    //     predicateType: 'equals',
    //   },
    // ],
  ];

  const applyLayers = R.pipe(
    layerFunctions.joins,
    layerFunctions.filters(conditions, boardData),
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
