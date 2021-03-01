const aws = require('aws-sdk');
const env = require('../env');

const spacesEndpoint = new aws.Endpoint('nyc3.digitaloceanspaces.com');
const awsConfig = new aws.Config({
  endpoint: spacesEndpoint,
  accessKeyId: env.SPACES_KEY,
  secretAccessKey: env.SPACES_SECRET,
});

const s3 = new aws.S3(awsConfig);

module.exports = s3;
