const R = require('ramda');

const findMax = R.reduce(
  (acc, curr) => (acc !== undefined ? (curr > acc ? curr : acc) : curr),
  undefined,
);

module.exports = findMax;
