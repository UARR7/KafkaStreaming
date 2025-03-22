// const WebSocket = require('ws');
// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'node-stream',
//   brokers: ['localhost:9092'],
// });

// const consumer = kafka.consumer({ groupId: 'order-group' });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on('connection', (ws) => {
//   console.log('Client connected');

//   ws.on('close', () => console.log('Client disconnected'));
// });

// // ✅ Start Kafka Consumer only once
// const run = async () => {
//   await consumer.connect();
//   await consumer.subscribe({ topic: 'orders', fromBeginning: true });

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       const msg = message.value.toString();

//       // ✅ Broadcast message to all connected clients
//       wss.clients.forEach((client) => {
//         if (client.readyState === WebSocket.OPEN) {
//           client.send(msg);
//         }
//       });
//     },
//   });
// };

// run().catch(console.error);

////////////////////////
// const WebSocket = require('ws');
// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'node-stream',
//   brokers: ['localhost:9092'],
// });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: 'order-group' });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on('connection', (ws) => {
//   console.log('✅ Client connected');
// });

// const run = async () => {
//   await producer.connect();
//   await consumer.connect();
//   await consumer.subscribe({ topic: 'orders', fromBeginning: true });

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       const consumedMessage = JSON.parse(message.value.toString());
//       console.log("📩 Consumed:", consumedMessage);

//       wss.clients.forEach((client) => {
//         if (client.readyState === WebSocket.OPEN) {
//           client.send(JSON.stringify({ type: 'consumed', data: consumedMessage }));
//         }
//       });
//     },
//   });
// };

// const produceMessage = async () => {
//   try {
//     if (!producer.isConnected()) {
//       console.log("🔄 Reconnecting Kafka Producer...");
//       await producer.connect();
//     }

//     const order = {
//       orderId: Math.random(),
//       status: 'Processing',
//     };

//     await producer.send({
//       topic: 'orders',
//       messages: [{ value: JSON.stringify(order) }],
//     });

//     console.log("📤 Produced:", order);

//     wss.clients.forEach((client) => {
//       if (client.readyState === WebSocket.OPEN) {
//         client.send(JSON.stringify({ type: 'produced', data: order }));
//       }
//     });

//     setTimeout(produceMessage, 3000); // Producing messages every 3 seconds
//   } catch (error) {
//     console.error("❌ Kafka Producer Error:", error);
//   }
// };

// run().catch(console.error);
// produceMessage();
/////////////
// const WebSocket = require('ws');
// const { Kafka } = require('kafkajs');

// const kafka = new Kafka({
//   clientId: 'node-stream',
//   brokers: ['localhost:9092'],
// });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: 'order-group' });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on('connection', (ws) => {
//   console.log('✅ Client connected');
// });

// const run = async () => {
//   await producer.connect();
//   await consumer.connect();
//   await consumer.subscribe({ topic: 'orders', fromBeginning: true });

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       const consumedMessage = JSON.parse(message.value.toString());
//       console.log("📩 Consumed:", consumedMessage);

//       // Send consumer messages via WebSocket
//       wss.clients.forEach((client) => {
//         if (client.readyState === WebSocket.OPEN) {
//           client.send(JSON.stringify({ type: 'consumed', data: consumedMessage }));
//         }
//       });
//     },
//   });
// };

// const produceMessage = async () => {
//   try {
//     if (!producer.isConnected()) {
//       console.log("🔄 Reconnecting Kafka Producer...");
//       await producer.connect();
//     }

//     const order = {
//       orderId: Math.random(),
//       status: 'Processing',
//     };

//     await producer.send({
//       topic: 'orders',
//       messages: [{ value: JSON.stringify(order) }],
//     });

//     console.log("📤 Produced:", order);

//     // 🔥 FIX: Send producer messages via WebSocket
//     wss.clients.forEach((client) => {
//       if (client.readyState === WebSocket.OPEN) {
//         client.send(JSON.stringify({ type: 'produced', data: order }));
//       }
//     });

//     setTimeout(produceMessage, 3000); // Producing messages every 3 seconds
//   } catch (error) {
//     console.error("❌ Kafka Producer Error:", error);
//   }
// };

// run().catch(console.error);
// produceMessage();

// const WebSocket = require("ws");
// const { Kafka } = require("kafkajs");

// const kafka = new Kafka({ clientId: "node-stream", brokers: ["localhost:9092"] });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: "order-group" });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on("connection", async (ws) => {
//   console.log("✅ Client connected");

//   // Produce a message
//   const produceMessage = async () => {
//     const order = { orderId: Math.random(), status: "Processing" };
//     await producer.send({
//       topic: "orders",
//       messages: [{ value: JSON.stringify(order) }],
//     });
//     ws.send(JSON.stringify({ type: "produced", data: order })); // Send to frontend
//     console.log("🔥 Produced:", order);
//   };

//   // Consume messages
//   consumer.run({
//     eachMessage: async ({ message }) => {
//       const order = JSON.parse(message.value.toString());
//       ws.send(JSON.stringify({ type: "consumed", data: order })); // Send to frontend
//       console.log("✅ Consumed:", order);
//     },
//   });

//   // Send a new message every 5 seconds
//   setInterval(produceMessage, 5000);

//   ws.on("close", () => console.log("❌ Client disconnected"));
// });

// // Start Kafka producer and consumer
// // const run = async () => {
// //   await producer.connect();
// //   console.log("🚀 Producer connected");
// //   await consumer.connect();
// //   await consumer.subscribe({ topic: "orders", fromBeginning: true });
// // };
// const run = async () => {
//     await producer.connect();
//     console.log("🚀 Producer connected");
  
//     await consumer.connect();
//     await consumer.subscribe({ topic: "orders", fromBeginning: true });
  
//     console.log("✅ Consumer subscribed");
    
//     await consumer.run({
//       eachMessage: async ({ message }) => {
//         const order = JSON.parse(message.value.toString());
//         console.log("✅ Consumed:", order);
  
//         // Send the consumed message to all WebSocket clients
//         wss.clients.forEach((client) => {
//           if (client.readyState === WebSocket.OPEN) {
//             client.send(JSON.stringify({ type: "consumed", data: order }));
//           }
//         });
//       },
//     });
//   };
  
//   run().catch(console.error);
  

// run().catch(console.error);



// const WebSocket = require("ws");
// const { Kafka } = require("kafkajs");

// const kafka = new Kafka({ clientId: "node-stream", brokers: ["localhost:9092"] });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: "order-group" });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on("connection", (ws) => {
//   console.log("✅ Client connected");
// });

// // ✅ Start Kafka producer and consumer once
// const run = async () => {
//   await producer.connect();
//   console.log("🚀 Producer connected");

//   await consumer.connect();
//   await consumer.subscribe({ topic: "orders"});
//   console.log("✅ Consumer subscribed");

//   // ✅ Kafka consumer runs independently of WebSocket
//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       let order = JSON.parse(message.value.toString());

//       // 🔄 Update status to "Consumed"
//       order.status = "Consumed";

//       console.log("✅ Updated & Consumed:", order);

//       // ✅ Send updated message to WebSocket clients
//       wss.clients.forEach((client) => {
//         if (client.readyState === WebSocket.OPEN) {
//           client.send(JSON.stringify({ type: "consumed", data: order }));
//         }
//       });

//       // ✅ Produce the updated message back to Kafka (Optional: Store processed data)
//       await producer.send({
//         topic: "processed-orders", // Store processed messages separately
//         messages: [{ value: JSON.stringify(order) }],
//       });

//       console.log("🔥 Sent to processed-orders topic:", order);
//     },
//   });
// };

// run().catch(console.error);

// // ✅ Producing messages every 5 seconds
// setInterval(async () => {
//   const order = { orderId: Math.random(), status: "Produced" };

//   await producer.send({
//     topic: "orders",
//     messages: [{ value: JSON.stringify(order) }],
//   });

//   console.log("🔥 Produced:", order);

//   // ✅ Send produced order to WebSocket
//   wss.clients.forEach((client) => {
//     if (client.readyState === WebSocket.OPEN) {
//       client.send(JSON.stringify({ type: "produced", data: order }));
//     }
//   });
// }, 5000);

//////
// const WebSocket = require("ws");
// const { Kafka } = require("kafkajs");

// const kafka = new Kafka({ clientId: "node-stream", brokers: ["localhost:9092"] });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: "order-group" });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on("listening", () => {
//   console.log("✅ WebSocket Server running on ws://localhost:8080");
// });

// // ✅ Possible statuses
// const STATUSES = ["Processing", "Ready", "Delivered"];

// // ✅ Send message to WebSocket clients
// const broadcast = (type, data) => {
//   wss.clients.forEach((client) => {
//     if (client.readyState === WebSocket.OPEN) {
//       client.send(JSON.stringify({ type, data }));
//     }
//   });
// };

// // ✅ Start Kafka producer and consumer
// const run = async () => {
//   await producer.connect();
//   console.log("🚀 Producer connected");

//   await consumer.connect();
//   await consumer.subscribe({ topic: "orders" });
//   console.log("✅ Consumer subscribed");

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       let order = JSON.parse(message.value.toString());

//       // 🔄 Assign a random status when consuming
//       order.status = STATUSES[Math.floor(Math.random() * STATUSES.length)];

//       console.log("✅ Consumed & Updated:", order);

//       // ✅ Send to WebSocket
//       broadcast("consumed", order);

//       // ✅ Produce updated message to Kafka
//       await producer.send({
//         topic: "processed-orders",
//         messages: [{ value: JSON.stringify(order) }],
//       });

//       console.log("🔥 Sent to processed-orders topic:", order);
//     },
//   });
// };

// run().catch(console.error);

// // ✅ Producing messages every 5 seconds
// setInterval(async () => {
//   const order = {
//     orderId: Math.floor(Math.random() * 1000), // Random Order ID
//     status: "Produced",
//   };

//   await producer.send({
//     topic: "orders",
//     messages: [{ value: JSON.stringify(order) }],
//   });

//   console.log("🔥 Produced:", order);

//   // ✅ Send produced order to WebSocket
//   broadcast("produced", order);
// }, 5000);
//////////

// const WebSocket = require("ws");
// const { Kafka } = require("kafkajs");

// const kafka = new Kafka({ clientId: "node-stream", brokers: ["localhost:9092"] });

// const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: "order-group" });

// const wss = new WebSocket.Server({ port: 8080 });

// wss.on("listening", () => {
//   console.log("✅ WebSocket Server running on ws://localhost:8080");
// });

// // ✅ Possible statuses
// const STATUSES = ["Processing", "Ready", "Delivered"];

// // ✅ Send message to WebSocket clients
// const broadcast = (type, data) => {
//   wss.clients.forEach((client) => {
//     if (client.readyState === WebSocket.OPEN) {
//       client.send(JSON.stringify({ type, data }));
//     }
//   });
// };

// // ✅ Start Kafka producer and consumer
// const run = async () => {
//   await producer.connect();
//   console.log("🚀 Producer connected");

//   await consumer.connect();
//   await consumer.subscribe({ topic: "orders" });
//   console.log("✅ Consumer subscribed");

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       let order = JSON.parse(message.value.toString());

//       console.log("✅ Consumed:", order);

//       // ✅ Send to WebSocket
//       broadcast("consumed", order);

//       // ✅ Produce the same message to processed-orders topic
//       await producer.send({
//         topic: "processed-orders",
//         messages: [{ value: JSON.stringify(order) }],
//       });

//       console.log("🔥 Sent to processed-orders topic:", order);
//     },
//   });
// };

// run().catch(console.error);

// // ✅ Producing messages every 5 seconds
// setInterval(async () => {
//   const order = {
//     orderId: Math.floor(Math.random() * 1000), // Random Order ID
//     status: STATUSES[Math.floor(Math.random() * STATUSES.length)], // ✅ Assign status in producer
//   };

//   await producer.send({
//     topic: "orders",
//     messages: [{ value: JSON.stringify(order) }],
//   });

//   console.log("🔥 Produced:", order);

//   // ✅ Send produced order to WebSocket
//   broadcast("produced", order);
// }, 5000);


const WebSocket = require("ws");
const { Kafka } = require("kafkajs");

const kafka = new Kafka({ clientId: "node-stream", brokers: ["localhost:9092"] });

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-group" });

const wss = new WebSocket.Server({ port: 8080 });

wss.on("listening", () => {
  console.log("✅ WebSocket Server running on ws://localhost:8080");
});

const orders = {}; // Store orders for status updates

const broadcast = (type, data) => {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type, data }));
    }
  });
};

const run = async () => {
  await producer.connect();
  console.log("🚀 Producer connected");

  await consumer.connect();
  await consumer.subscribe({ topic: "orders" });
  console.log("✅ Consumer subscribed");

  await consumer.run({
    eachMessage: async ({ message }) => {
      let order = JSON.parse(message.value.toString());
      order.status = "Processing"; // Always set to "Processing" in consumer
      orders[order.orderId] = order;
      console.log("✅ Consumed & Updated:", order);

      broadcast("consumed", order);

      await producer.send({
        topic: "processed-orders",
        messages: [{ value: JSON.stringify(order) }],
      });
    },
  });
};

wss.on("connection", (ws) => {
  ws.on("message", async (msg) => {
    const { orderId, status } = JSON.parse(msg);
    if (orders[orderId] && ["Processing", "Ready", "Delivered"].includes(status)) {
      orders[orderId].status = status;
      broadcast("updated", orders[orderId]);
      await producer.send({
        topic: "processed-orders",
        messages: [{ value: JSON.stringify(orders[orderId]) }],
      });
      console.log("🔄 Updated Order:", orders[orderId]);
    }
  });
});

run().catch(console.error);

setInterval(async () => {
  const order = {
    orderId: Math.floor(Math.random() * 1000),
    status: "Order Received",
  };
  orders[order.orderId] = order;

  await producer.send({
    topic: "orders",
    messages: [{ value: JSON.stringify(order) }],
  });

  console.log("🔥 Produced:", order);
  broadcast("produced", order);
}, 5000);
