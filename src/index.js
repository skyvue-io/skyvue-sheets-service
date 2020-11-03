const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

require('dotenv').config();

app.get('/', (req, res) => {
  res.send('Datasets service is alive!');
});

io.on('connection', socket => {
  console.log('a user connected', socket);
});

io.on('disconnect', socket => {
  console.log('disconnected');
});

http.listen(3030, () => {
  console.log('listening on *:3000');
});
