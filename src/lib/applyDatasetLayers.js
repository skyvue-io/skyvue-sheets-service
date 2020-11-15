const R = require('ramda');
const applyFilters = require('./layers/applyFilters');
const applyGrouping = require('./layers/applyGrouping');

const applyDatasetLayers = (layers, boardData) => {
  const groupings = {
    groupedBy: ['fc8d530e-41d2-43f0-87ce-9e30ab6f8c06'],
    columnAggregates: {
      '2d9dc775-d7f2-4acc-ba07-6f20493abac8': 'count',
    },
  };
  const filters = [
    // 'AND',
    // {
    //   key: '2d9dc775-d7f2-4acc-ba07-6f20493abac8',
    //   value: '1',
    //   predicateType: 'equals',
    // },
    // [
    //   'AND',
    //   {
    //     key: '2d9dc775-d7f2-4acc-ba07-6f20493abac8',
    //     value: '1',
    //     predicateType: 'equals',
    //   },
    //   {
    //     key: 'fc8d530e-41d2-43f0-87ce-9e30ab6f8c06',
    //     value: 'marvel/0001/002.jpg',
    //     predicateType: 'equals',
    //   },
    // ],
  ];

  const applyLayers = R.pipe(
    R.identity, // future, joins
    applyFilters(filters),
    applyGrouping(groupings),
    R.identity, // future, sortings,
    R.identity, // future, formatting,
  )(boardData);

  return applyLayers;
};

module.exports = applyDatasetLayers;
