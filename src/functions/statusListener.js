const { getClient } = require('./dbClient');

let client; // Persist the client to keep the LISTEN active

async function startNotificationListener(onMessage) {
  try {
    client = await getClient();
    console.log('PostgreSQL client connected for LISTEN');

    await client.query('LISTEN sensor_status');
    console.log('Listening on channel: sensor_status');

    client.on('notification', (msg) => {
      const payload = JSON.parse(msg.payload);
      console.log('Notification received:', payload);
      onMessage(payload);
    });

    client.on('error', (err) => {
      console.error('PostgreSQL notification listener error:', err);
      
    });
  } catch (err) {
    console.error('Failed to set up notification listener:', err);
  }
}

module.exports = { startNotificationListener };