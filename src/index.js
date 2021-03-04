const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const R = require('ramda');

const Sentry = require('@sentry/node');
const Tracing = require('@sentry/tracing');

Sentry.init({
  dsn: process.env.SENTRY_DSN,

  // We recommend adjusting this value in production, or using tracesSampler
  // for finer control
  tracesSampleRate: 1.0,
});

Sentry.startTransaction({
  op: 'idk',
  name: "Not sure what I'm doing",
});

const { Dataset, initial_layers } = require('./Dataset');
const loadDataset = require('./services/loadDataset');
const applyDatasetLayers = require('./lib/applyDatasetLayers');

const connections = {};
const idleSaveTimer = {};
const DEFAULT_SLICE_START = 0;
const DEFAULT_SLICE_END = 100;

app.get('/', (req, res) => {
  res.send(`Datasets service is alive!`);
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

  const refreshInView = async cnxn => {
    try {
      const slice = await cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END);
      socket.emit('slice', slice);
      socket.emit('csvEstimate', await cnxn.estCSVSize());
      socket.emit('meta', cnxn.meta);
    } catch (e) {
      socket.emit('boardError', {
        message: e.message,
        type: e.type,
        target: e._id,
      });
    }
  };

  socket.on('loadDataset', async () => {
    await cnxn.load();
    const slice = await cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END);
    socket.emit('initialDatasetReceived', slice);
    socket.emit('csvEstimate', await cnxn.estCSVSize());
    socket.emit('meta', cnxn.meta);
  });

  socket.on('head', async () => {
    socket.emit('head', cnxn.head);
  });

  socket.on('queryBoardHeaders', async datasetId => {
    const dataset = datasetId ? await loadDataset(datasetId) : {};

    socket.emit(
      'returnQueryBoardHeaders',
      R.omit(
        ['rows'],
        applyDatasetLayers(datasetId, dataset.layers ?? initial_layers, {}, dataset),
      ),
    );
  });

  socket.on('getSlice', async ({ first, last }) => {
    cnxn.setSliceQueue(first, last ?? first + DEFAULT_SLICE_END);
    if (cnxn.sliceQueue?.[0] === first) return;
    const response = await cnxn.getSlice(first, last ?? first + DEFAULT_SLICE_END);
    cnxn.clearSliceQueue();
    socket.emit('slice', response);
  });

  socket.on('layer', async ({ layerKey, layerData }) => {
    try {
      cnxn.addLayer(layerKey, layerData);
      await refreshInView(cnxn);
      saveAfterDelay();
    } catch (e) {
      socket.emit('boardError', {
        message: e.message,
        type: e.type,
        target: e._id,
      });
    }
  });

  socket.on('clearLayers', async params => {
    cnxn.clearLayers(params);
    await refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('syncLayers', async layers => {
    cnxn.syncLayers(layers);
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
    await refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('checkoutToVersion', async ({ versionId, start, end, direction }) => {
    const data = cnxn.checkoutToVersion(versionId, direction);
    if (data) {
      socket.emit('slice', await cnxn.getSlice(start, end));
      saveAfterDelay();
    }
  });
});

io.on('disconnect', socket => {
  Object.keys(connections).forEach(key => delete connections[key]);
});

http.listen(3030, () => {
  console.log('listening on *:3000');
});
