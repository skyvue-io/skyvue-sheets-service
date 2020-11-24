const safeParseNumber = input => {
  const parsed = parseInt(input, 10);
  // eslint-disable-next-line no-restricted-globals
  return isNaN(parsed) ? input : parsed;
};

module.exports = safeParseNumber;
