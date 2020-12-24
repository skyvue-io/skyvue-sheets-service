const safeParseNumber = input => {
  if (input.match(/^-?\d+$/)) {
    return parseInt(input, 10);
  }
  if (input.match(/^\d+\.\d+$/)) {
    return parseFloat(input);
  }
  return input;
};

module.exports = safeParseNumber;
