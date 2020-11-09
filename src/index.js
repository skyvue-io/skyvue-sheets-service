const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const path = require('path');
const Session = require('./Session');

const connections = {};

app.get('/', (req, res) => {
  res.send('Datasets service is alive!');
});

io.on('connection', async socket => {
  const { datasetId, userId } = socket.handshake.query;

  if (datasetId && userId) {
    socket.join(datasetId);
    if (!connections[datasetId]) {
      connections[datasetId] = Session({
        datasetId,
        userId,
      });
    }
  }

  const cnxn = connections[datasetId];

  socket.on('loadDataset', async () => {
    const data = await cnxn.load();
    socket.emit('initialDatasetReceived', data);
  });

  socket.on('diff', async data => {
    await cnxn.addChange(data);
    await cnxn.save();
    console.log('saving');
    socket.emit('returnDiff', { data: cnxn.baseState });
  });

  socket.on('saveDataset', async () => {
    await cnxn.save();
  });
});

io.on('disconnect', socket => {
  console.log('disconnected');
});

http.listen(3030, () => {
  console.log('listening on *:3000');
});
