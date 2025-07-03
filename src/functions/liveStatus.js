const WebSocket = require('ws');
const { startNotificationListener } = require('./statusListener');

const PORT = process.env.WS_PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

console.log(`WebSocket server started on ws://localhost:${PORT}`);

function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', (ws) => {
  console.log('Frontend connected to WebSocket');
  ws.isAlive = true;
  ws.on('pong', heartbeat);

  // Optionally, send a welcome message or initial data here
});

// Heartbeat interval
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('Terminating dead WebSocket connection');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000); // 30 seconds

wss.on('close', () => {
  clearInterval(interval);
});

function broadcastToClients(payload) {
  const message = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Start listening to DB notifications and broadcast to all clients
startNotificationListener(broadcastToClients);