const { isValid } = require('date-fns');

const parseDataType = value => {
  if (/^-?(0|[1-9]\d*)(\.\d+)?$/.test(value)) {
    return 'number';
  }
  if (isValid(new Date(value))) {
    return 'date';
  }
  return 'string';
};

module.exports = parseDataType;
