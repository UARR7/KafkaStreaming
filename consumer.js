const { Kafka } = require('kafkajs');

const kafka = new Kafka({ clientId: 'node-stream', brokers: ['localhost:9092'] });

const consumer = kafka.consumer({ groupId: 'order-group' });

const consumeMessages = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'orders'});

    await consumer.run({
        eachMessage: async ({ message }) => {
            console.log('Consumed:', message.value.toString());
        },
    });
};

consumeMessages();
