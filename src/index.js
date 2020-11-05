const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const aws = require('aws-sdk');

require('dotenv').config();

const awsConfig = new aws.Config({
  region: 'us-west-1',
  accessKeyId: process.env.AWS_ACCESS_KEY,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new aws.S3(awsConfig);

app.get('/', (req, res) => {
  res.send('Datasets service is alive!');
});

io.on('connection', async socket => {
  const { datasetId, userId } = socket.handshake.query;

  if (datasetId && userId) {
    socket.join(datasetId);
  }

  socket.on('loadDataset', async () => {
    const s3Params = {
      Bucket: 'skyvue-datasets',
      Key: `${userId}-${datasetId}`,
    };

    await s3.headObject(s3Params).promise();
    const res = await s3.getObject(s3Params).promise();
    const data = JSON.parse(res.Body.toString('utf-8'));

    socket.emit('initialDatasetReceived', data);
  });
});

io.on('disconnect', socket => {
  console.log('disconnected');
});

http.listen(3030, () => {
  console.log('listening on *:3000');
});
