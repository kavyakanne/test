const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

const app = express();
const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://user:password@localhost:5672';
const QUEUE_NAME = 'events_queue';

let channel = null;

// Middleware
app.use(cors());
// Support both text/plain (from sendBeacon) and application/json
app.use(express.text({ type: 'text/plain' }));
app.use(express.json());

// Initialize RabbitMQ Connection
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, {
      durable: true // Ensure messages survive broker restarts
    });
    console.log('Connected to RabbitMQ API Producer');
  } catch (error) {
    console.error('Error connecting to RabbitMQ:', error);
    // Exit if RabbitMQ is not available; allow container orchestrator to restart
    process.exit(1); 
  }
}

connectRabbitMQ();

// Endpoint for receiving events
app.post('/collect', (req, res) => {
  if (!channel) {
    return res.status(503).json({ error: 'Message queue unavailable' });
  }

  let eventPayload = req.body;

  // Handle payload parsing depending on content-type
  if (typeof req.body === 'string') {
    try {
      eventPayload = JSON.parse(req.body);
    } catch (e) {
      return res.status(400).json({ error: 'Invalid JSON payload' });
    }
  }

  if (!eventPayload) {
     return res.status(400).json({ error: 'Missing payload' });
  }

  // Handle batch (array) or single event
  const events = Array.isArray(eventPayload) ? eventPayload : [eventPayload];
  const serverTimestamp = Date.now();

  events.forEach(event => {
    // Attach server timestamp if not present, and to track ingestion time
    event.serverReceivedAt = serverTimestamp;

    // Publish to RabbitMQ
    const status = channel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(event)),
      { persistent: true } 
    );
    console.log(`[API] Event ${event.eventType} sent to RabbitMQ: ${status}`);
  });

  // Respond quickly
  res.status(202).json({ success: true, count: events.length });
});

// Listener for channel errors
setTimeout(() => {
  if (channel) {
    channel.on('error', (err) => console.error('[API] RabbitMQ Channel Error:', err));
    channel.on('close', () => console.error('[API] RabbitMQ Channel Closed'));
  }
}, 1000);

// Start the server
app.listen(PORT, () => {
  console.log(`API Producer Server running on port ${PORT}`);
});
