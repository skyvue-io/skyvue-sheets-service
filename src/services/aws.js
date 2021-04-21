const aws = require('aws-sdk');

const awsConfig = new aws.Config({
  region: 'us-east-2',
  accessKeyId: process.env.AWS_ACCESSKEY,
  secretAccessKey: process.env.AWS_SECRET_ACCESSKEY,
});

const s3 = new aws.S3(awsConfig);

module.exports = s3;
