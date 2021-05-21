const R = require('ramda');

const chainPredicateMap = Object.freeze({
  AND: 'where',
  OR: 'orWhere',
});

const valuePredicateMap = Object.freeze({
  equals: { suffix: '', operator: undefined },
  notEquals: { suffix: 'Not', operator: undefined },

  notNull: { suffix: 'NotNull', operator: undefined },
  null: { suffix: 'Null', operator: undefined },
  contains: { suffix: 'In', operator: undefined },
  lessThan: { suffix: '', operator: '<' },
  lessThanEqualTo: { suffix: '', operator: '<=' },
  greaterThan: { suffix: '', operator: '>' },
  greaterThanEqualTo: { suffix: '', operator: '>=' },
});

const addEntryToQuery = (context, chainPredicate, { predicateType, key, value }) => {
  const { suffix, operator } = valuePredicateMap[predicateType];

  const predicate = `${chainPredicate}${suffix}`;

  if (predicateType === 'contains') {
    return context[predicate](
      key,
      value.split(',').map(v => v.trim()),
    );
  }

  if (operator) {
    return context[predicate](key, operator, value);
  }
  return context[predicate](key, value);
};

const whereRecursive = (context, parentPredicate, input) =>
  context[parentPredicate](function () {
    const context = this;
    const [chainPredicateKey, firstEntry, ...entries] = input;

    if (!chainPredicateKey || !firstEntry) {
      return;
    }

    const chainPredicate = chainPredicateMap[chainPredicateKey];

    addEntryToQuery(context, chainPredicate, firstEntry);

    entries.forEach(entry => {
      if (!Array.isArray(entry)) {
        return addEntryToQuery(context, chainPredicate, entry);
      }
      whereRecursive(context, chainPredicate, entry);
    });
  });

const makeFilterQuery = R.curry((filterLayer, knex_) => {
  const knex = knex_.clone();

  return whereRecursive(knex, 'where', filterLayer);
});

module.exports = makeFilterQuery;
