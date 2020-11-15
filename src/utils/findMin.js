const R = require('ramda');

const findMin = R.reduce(
  (acc, curr) => (acc !== undefined ? (curr < acc ? curr : acc) : curr),
  undefined,
);

module.exports = findMin;
