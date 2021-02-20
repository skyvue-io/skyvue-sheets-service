const { format } = require('date-fns');

const formatNumber = ({ desiredFormat, dataType, value, formatSettings }) => {
  if (value === undefined || value === null) return;
  const parsed = parseFloat(value);

  if (desiredFormat === 'currency') {
    const asCurrency = new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: formatSettings?.currencyCode ?? 'USD',
    }).format(parsed);

    return (formatSettings?.decimalPoints ?? 2) < 1
      ? asCurrency.replace(/\D00$/, '')
      : asCurrency;
  }

  if (desiredFormat === 'percent') {
    return `${(parsed * 100).toFixed(formatSettings?.decimalPoints ?? 2)}%`;
  }

  return formatSettings?.commas ?? true
    ? parsed.toLocaleString('en-US', {
        minimumFractionDigits: formatSettings?.decimalPoints ?? 2,
        maximumFractionDigits: formatSettings?.decimalPoints ?? 2,
      })
    : parsed.toFixed(formatSettings?.decimalPoints ?? 2);
};

const formatDate = ({ desiredFormat, value }) => {
  if (value === undefined || value === null) return;
  const date = new Date(value);
  switch (desiredFormat) {
    case 'iso string':
      return date.toISOString();
    case 'datetime':
      return date.toLocaleString();
    case 'locale date':
      return date.toLocaleDateString();
    case 'locale time':
      return date.toLocaleTimeString();
    default:
      return format(date, desiredFormat ?? 'MM-dd-yyyy');
  }
};

const formatValue = params => {
  const { value, dataType } = params;
  if (value === undefined || value === null) return;
  try {
    switch (dataType) {
      case 'date':
        return formatDate(params);
      case 'number':
        return formatNumber(params);
      default:
        return value;
    }
  } catch (e) {
    return value.toString();
  }
};

const formatValueFromBoardData = (colId, value, boardData) => {
  const column = boardData.columns.find(col => col._id === colId);
  const join = boardData.layers.joins.condition;
  const smartColumn = boardData.layers.smartColumns.find(col => col._id === colId);

  const { format, formatSettings } = column.isJoined
    ? join
    : column.isSmartColumn
    ? smartColumn
    : column;

  return formatValue({
    desiredFormat: format,
    dataType: column.dataType,
    value,
    formatSettings,
  });
};

module.exports = { formatValue, formatValueFromBoardData };
