const R = require('ramda');

const addLayer = (layerKey, layer, layers) => ({
  ...layers,
  [layerKey]: R.uniqWith(R.eqProps, layers ? [...layers[layerKey], layer] : [layer]),
});

module.exports = addLayer;
