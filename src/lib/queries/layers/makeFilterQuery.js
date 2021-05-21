const R = require('ramda');

const chainPredicateMap = {
  AND: 'where',
  OR: 'orWhere',
};

const whereRecursive = (context, parentPredicate, input) =>
  context[parentPredicate](function () {
    const context = this;
    const [chainPredicateKey, firstEntry, ...entries] = input;

    if (!chainPredicateKey || !firstEntry) {
      return;
    }

    const chainPredicate = chainPredicateMap[chainPredicateKey];
    context[chainPredicate](firstEntry.key, firstEntry.value);

    entries.forEach(entry => {
      if (!Array.isArray(entry)) {
        return context[chainPredicate](entry.key, entry.value);
      }
      whereRecursive(context, chainPredicate, entry);
    });
  });

const makeFilterQuery = R.curry((filterLayer, knex_) => {
  const knex = knex_.clone();

  return whereRecursive(knex, 'where', filterLayer);
});

module.exports = makeFilterQuery;
