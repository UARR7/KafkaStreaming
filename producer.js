const { Kafka } = require('kafkajs');

const kafka = new Kafka({ clientId: 'node-stream', brokers: ['localhost:9092'] });

const producer = kafka.producer();

const produceMessages = async () => {
    await producer.connect();
    setInterval(async () => {
        const message = { value: JSON.stringify({ orderId: Math.random(), status: "Processing" }) };
        await producer.send({ topic: 'orders', messages: [message] });
        console.log('Produced:', message.value);
    }, 2000);
};

produceMessages();
