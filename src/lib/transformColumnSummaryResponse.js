const R = require('ramda');
const safeParseNumber = require('../utils/safeParseNumber');

const UUID_REGEX = /\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b/g;

const transformColumnSummaryResponse = ({ rows = [] }) => {
  const [summary] = rows;

  const colIds = R.pipe(
    R.keys,
    R.map(R.pipe(R.match(UUID_REGEX), R.nth(0))),
    R.uniq,
  )(summary);

  return R.pipe(
    R.reduce(
      (acc, columnId) =>
        R.assoc(columnId, {
          columnId,
        })(acc),
      {},
    ),
    R.map(entry =>
      R.pipe(
        R.keys,
        R.filter(key => key.includes(entry.columnId)),
        R.map(key => {
          const [colId] = key.match(UUID_REGEX);
          const predicateName = key.slice(0, key.length - `-${colId}`.length);
          return {
            ...entry,
            // todo fix this. This is terrible technique but I'm tired.
            [predicateName === 'uniquevalues'
              ? 'uniqueValues'
              : predicateName]: safeParseNumber(summary[key]),
          };
        }),
      )(summary),
    ),
    R.map(R.mergeAll),
  )(colIds);
};

module.exports = transformColumnSummaryResponse;
