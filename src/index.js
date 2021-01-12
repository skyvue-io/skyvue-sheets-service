const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const Dataset = require('./Dataset');

const connections = {};
const idleSaveTimer = {};
const DEFAULT_SLICE_START = 0;
const DEFAULT_SLICE_END = 100;

app.get('/', (req, res) => {
  res.send('Datasets service is alive!');
});

io.on('connection', async socket => {
  const { datasetId, userId } = socket.handshake.query;

  if (datasetId && userId) {
    socket.join(datasetId);
    if (!connections[datasetId]) {
      connections[datasetId] = Dataset({
        datasetId,
        userId,
      });
    }
  }

  const cnxn = connections[datasetId];

  const saveAfterDelay = () => {
    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.queueFunc(cnxn.save);
    idleSaveTimer[datasetId] = setTimeout(() => {
      cnxn.save();
      cnxn.clearFuncQueue();
    }, 5000);
  };

  const refreshInView = cnxn => {
    const slice = cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END);
    socket.emit('slice', slice);
    socket.emit('csvEstimate', cnxn.estCSVSize);
    socket.emit('meta', cnxn.meta);
  };

  socket.on('loadDataset', async () => {
    await cnxn.load();
    const slice = cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END);
    socket.emit('initialDatasetReceived', slice);
    socket.emit('csvEstimate', cnxn.estCSVSize);
    socket.emit('meta', cnxn.meta);
  });

  socket.on('head', async () => {
    socket.emit('head', cnxn.head);
  });

  socket.on('getSlice', async ({ first, last }) => {
    socket.emit('slice', cnxn.getSlice(first, last ?? first + DEFAULT_SLICE_END));
  });

  socket.on('layer', async ({ layerKey, layerData }) => {
    cnxn.addLayer(layerKey, layerData);
    refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('clearLayers', async params => {
    cnxn.clearLayers(params);
    refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('diff', async data => {
    await cnxn.addDiff(data);
    saveAfterDelay();
  });

  socket.on('unload', async () => {
    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.runQueuedFunc();
  });

  socket.on('exportToCsv', async ({ title, quantity }) => {
    const s3Urls = await cnxn.exportToCSV(title, quantity);
    socket.emit('downloadReady', s3Urls);
  });

  socket.on('saveDataset', async () => {
    await cnxn.save();
  });

  socket.on('saveAsNew', async ({ newDatasetId }) => {
    await cnxn.saveAsNew(newDatasetId);
    socket.emit('duplicateReady', { _id: newDatasetId });
  });

  socket.on('saveToHistory', change => {
    cnxn.saveToHistory(change);
  });

  socket.on('toggleLayer', async ({ toggle, visible }) => {
    await cnxn.toggleLayer(toggle, visible);
    refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('checkoutToVersion', ({ versionId, start, end, direction }) => {
    const data = cnxn.checkoutToVersion(versionId, direction);
    if (data) {
      socket.emit('slice', cnxn.getSlice(start, end));
      saveAfterDelay();
    }
  });
});

io.on('disconnect', socket => {
  console.log('disconnected');
});

http.listen(3030, () => {
  console.log('listening on *:3000');
});
