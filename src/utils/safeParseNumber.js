const safeParseNumber = input => {
  if (!input) return input;
  try {
    if (input.toString().match(/^-?\d+$/)) {
      return parseInt(input, 10);
    }
    if (input.toString().match(/^\d+\.\d+$/)) {
      return parseFloat(input);
    }
    return input;
  } catch (e) {
    return input;
  }
};

module.exports = safeParseNumber;
