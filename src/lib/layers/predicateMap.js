const R = require('ramda');

const predicateMap = {
  // eslint-disable-next-line eqeqeq
  equals: (a, b) => a == b,
  // eslint-disable-next-line eqeqeq
  notEquals: (a, b) => a != b,
  contains: (a, b) => b.includes(a),
  lessThan: (a, b) => a < b,
  lessThanEqualTo: (a, b) => a <= b,
  greaterThan: (a, b) => a > b,
  greaterThanEqualTo: (a, b) => a >= b,
  dateBetween: R.__,
};

module.exports = predicateMap;
