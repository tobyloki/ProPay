const bodyParser = require('body-parser');
const express = require('express');
const { Kafka } = require('kafkajs');
const assert = require('assert');

const app = express();
app.use(bodyParser.json());
app.use((req, res, next) => {
    res.header('Content-Type', 'application/json');
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS,POST,PUT");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization");
    next();
});
const port = process.env.PORT || 5000;

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});
const producer = kafka.producer();

init();
async function init(){
    await producer.connect();
    console.log('Kafka connected');
}

app.listen(port, () => console.log(`App listening on http://localhost:${port}`));

app.post('/pay', async (req, res) => {
    const message = req.body;
    assert(message != null);

    const response = await producer.send({
        topic: 'TestTopic',
        messages: [
            {
                value: JSON.stringify(message)
            }
        ]
    });
    console.log('Kafka message sent');
    console.log(response);

    res.send({
        response
    });
});
