require('dotenv').config();

const express = require('express');

const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const csv = require('csvtojson');

const R = require('ramda');
const Sentry = require('@sentry/node');
const Tracing = require('@sentry/tracing');

const loadCompiledDataset = require('./services/loadCompiledDataset');

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
const DEFAULT_SLICE_END = 200;
const ORIGIN_ALLOW_LIST =
  process.env.NODE_ENV === 'development'
    ? ['http://localhost:3000', 'http://localhost:8888']
    : ['https://app.skyvue.io'];

app.use(express.json());
app.use(
  express.urlencoded({
    extended: true,
  }),
);
app.get('/', (req, res) => {
  res.send(`Datasets service is alive!!`);
});

app.use('/api', require('./routes'));

io.on('connection', async socket => {
  const { datasetId, userId } = socket.handshake.query;

  if (
    datasetId &&
    userId &&
    ORIGIN_ALLOW_LIST.includes(socket.handshake?.headers?.origin)
  ) {
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
    // todo this might should just save column data so long as cnxn.unload() is reliable.
    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.queueFunc(cnxn.saveHead);
    idleSaveTimer[datasetId] = setTimeout(() => {
      // todo interact with batch update service here?
      cnxn.saveHead();
      cnxn.clearFuncQueue();
    }, 5000);
  };

  const refreshInView = async cnxn => {
    try {
      await cnxn.load();
      const slice = await cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END);
      socket.emit('setBoardData', slice);
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
    const slice = await cnxn.getSlice(DEFAULT_SLICE_START, DEFAULT_SLICE_END, {
      useCached: true,
    });
    socket.emit('initialDatasetReceived', slice);
    socket.emit('csvEstimate', 200); // todo fixme
    socket.emit('meta', cnxn.meta);
  });

  socket.on('queryBoardHeaders', async datasetId => {
    // todo can just return column data from s3, and figure out what columns will be there
    const queriedDataset = await loadCompiledDataset(datasetId, undefined, {
      onlyHead: true,
    });

    socket.emit('returnQueryBoardHeaders', queriedDataset);
  });

  // Only use this route to return a slice from the cached dataset (e.g. on a scroll event).
  // Don't call it expecting any changes to the compilation.
  socket.on('getRowSlice', async ({ first, last }) => {
    if (cnxn.lastSlice?.[0] === first) return;
    cnxn.setLastSlice(first, last ?? first + DEFAULT_SLICE_END);
    const response = await cnxn.getSlice(first, last ?? first + DEFAULT_SLICE_END, {
      useCached: true,
    });
    socket.emit('appendRows', response.rows);
  });

  socket.on('getBoardDataFromIndex', async ({ first, last }) => {
    const response = await cnxn.getSlice(first, last ?? first + DEFAULT_SLICE_END, {
      useCached: true,
    });
    socket.emit('setBoardData', response);
  });

  socket.on('layer', async ({ layerKey, layerData, refresh = true }) => {
    try {
      cnxn.addLayer(layerKey, layerData);
      if (refresh) {
        await refreshInView(cnxn);
      }
      saveAfterDelay();
    } catch (e) {
      socket.emit('boardError', {
        message: e.message,
        type: e.type,
        target: e._id,
      });
    }
  });

  socket.on('setDeletedObjects', deletedObjects => {
    cnxn.setDeletedObjects(deletedObjects);
    saveAfterDelay();
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

  socket.on('setColOrder', colOrder => {
    cnxn.setColOrder(colOrder);
  });

  socket.on('diff', async data => {
    await cnxn.addDiff(data);
    saveAfterDelay();
  });

  socket.on('datadump', async file => {
    // todo We should likely offload this to postgres
    const csvAsJson = await csv().fromString(file.toString());
    cnxn.setLastAppend(csvAsJson);
    socket.emit('appendPreview', {
      meta: {
        length: csvAsJson.length,
      },
      records: [cnxn.lastAppend?.[0], R.last(cnxn.lastAppend)],
    });
  });

  socket.on('importLastAppended', async importSettings => {
    await cnxn.importLastAppended(importSettings);
    await refreshInView(cnxn);
    saveAfterDelay();
  });

  socket.on('unload', async () => {
    // todo will need to call cnxn.unload();
    clearTimeout(idleSaveTimer[datasetId]);
    cnxn.runQueuedFunc();
  });

  socket.on('exportToCsv', async ({ title, quantity }) => {
    const s3Urls = await cnxn.exportToCSV(title, quantity);
    socket.emit('downloadReady', s3Urls);
  });

  socket.on('addUnsavedChange', change => {
    cnxn.addToUnsavedChanges(change);
    saveAfterDelay();
  });

  socket.on('saveRows', async () => {
    await cnxn.saveRows();
    refreshInView(cnxn);
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
      socket.emit('setBoardData', await cnxn.getSlice(start, end));
      saveAfterDelay();
    }
  });
});

io.on('disconnect', socket => {
  // todo call unload()
  Object.keys(connections).forEach(key => delete connections[key]);
});

const shouldRunMonitor = false;
if (shouldRunMonitor && process.env.NODE_ENV === 'development') {
  // todo should this ping a data store for some period of time so we can get some basic analytics? Probably
  setInterval(() => {
    const formatMemoryUsage = data =>
      `${Math.round((data / 1024 / 1024) * 100) / 100} MB`;

    const memoryData = process.memoryUsage();

    const memoryUsage = {
      rss: `${formatMemoryUsage(
        memoryData.rss,
      )} -> Resident Set Size - total memory allocated for the process execution`,
      heapTotal: `${formatMemoryUsage(
        memoryData.heapTotal,
      )} -> total size of the allocated heap`,
      heapUsed: `${formatMemoryUsage(
        memoryData.heapUsed,
      )} -> actual memory used during the execution`,
      external: `${formatMemoryUsage(memoryData.external)} -> V8 external memory`,
    };

    console.log(memoryUsage);
  }, 10000);
}

const PORT = process.env.PORT || 8080;
http.listen(PORT, () => {
  console.log(`listening on *:${PORT}`);
});
