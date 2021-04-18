const R = require('ramda');

const pipePromises = R.unapply(R.pipeWith(R.andThen));

module.exports = pipePromises;
