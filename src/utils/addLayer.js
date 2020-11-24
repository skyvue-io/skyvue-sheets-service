const R = require('ramda');

const addLayer = (layerKey, layer, layers) => ({
  ...layers,
  [layerKey]: layer,
});

module.exports = addLayer;
