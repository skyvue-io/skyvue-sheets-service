const s3 = require('./aws');

const selectContentFromS3 = async (
  s3Params,
  query = 'SELECT * FROM S3Object limit 500',
) => {
  const params = {
    ...s3Params,
    ExpressionType: 'SQL',
    Expression: query,
    InputSerialization: {
      CSV: {
        FileHeaderInfo: 'NONE',
        RecordDelimiter: '\n',
        FieldDelimiter: ',',
      },
    },
    OutputSerialization: {
      CSV: {},
    },
  };

  const res = await s3.selectObjectContent(params).promise();
  const events = res.Payload;
  let response = '';

  for await (const event of events) {
    if (event.Records) {
      response += event.Records.Payload.toString();
    }
  }

  return response;
};

module.exports = selectContentFromS3;
