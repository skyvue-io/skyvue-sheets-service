const R = require('ramda');
const applyFilters = require('./layers/applyFilters');
const applyGrouping = require('./layers/applyGrouping');
const applySortings = require('./layers/applySortings');

const groupings = {
  // groupedBy: ['fc8d530e-41d2-43f0-87ce-9e30ab6f8c06'],
  // columnAggregates: {
  //   '2d9dc775-d7f2-4acc-ba07-6f20493abac8': 'count',
  // },
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

const sortings = [
  {
    key: '1dd4dfbf-6eff-48c2-bdff-949a1d42ee78',
    direction: 'desc',
  },
];

const applyDatasetLayers = (layers, boardData) =>
  R.pipe(
    R.identity, // future, joins
    applyFilters(layers.filters),
    applyGrouping(layers.groupings),
    applySortings(sortings),
    R.identity, // future, formatting,
  )(boardData);

module.exports = applyDatasetLayers;
