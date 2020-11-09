const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const { emit } = require('process');
const Session = require('./Session');

const connections = {};
const idleSaveTimer = {};

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
    await cnxn.load();
    const slice = cnxn.getSlice(0, 30);
    socket.emit('initialDatasetReceived', slice);
  });

  socket.on('getSlice', async ({ first, last }) => {
    socket.emit('slice', cnxn.getSlice(first, last));
  });

  socket.on('diff', async data => {
    await cnxn.addDiff(data);

    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.queueFunc(cnxn.save);
    idleSaveTimer[datasetId] = setTimeout(() => {
      cnxn.save();
      cnxn.clearFuncQueue();
    }, 5000);

    socket.emit('returnDiff', { data: cnxn.baseState });
  });

  socket.on('unload', async () => {
    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.runQueuedFunc();
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
