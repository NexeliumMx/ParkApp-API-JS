const WebSocket = require('ws');
const { startNotificationListener } = require('./statusListener');

const PORT = process.env.WS_PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

console.log(`WebSocket server started on ws://localhost:${PORT}`);

wss.on('connection', (ws) => {
  console.log('Frontend connected to WebSocket');
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