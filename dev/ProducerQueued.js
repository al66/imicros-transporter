const { Kafka } = require("kafkajs");
const { v4: uuid } = require("uuid");

// Create the client with the broker list
const kafka = new Kafka({
    clientId: uuid(),
    brokers: ["192.168.2.124:9092"],
    ssl: null,      // refer to kafkajs documentation
    sasl: null,     // refer to kafkajs documentation
    connectionTimeout: 1000,
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});

let producer;

const topic = "performance-" + uuid();
const numPartitions = 20;
const n = 30000;
const p = 10000;
let maxBatchSize = 1000;
let count = 0;
let commit = [];

const createTopic = async function (topic, numPartitions) {
    let admin = await kafka.admin();
    await admin.connect();
    let topics = [{
        topic: topic,
        numPartitions: numPartitions,
        replicationFactor: 1
    }];
    try {
        await admin.createTopics({
            // validateOnly: true,  // default false
            waitForLeaders: true,   // default true
            timeout: 30000,         // default: 1000 (ms)
            topics: topics,
        });
        await admin.disconnect();
        console.log("Topic created: " + topic + " Partitions: " + numPartitions);
    } catch (err) {
        await admin.disconnect();
        if (err.error === "Topic with this name already exists") return { topic: topic };
        console.log("Failed to create topic " + topic);
        return { topic: topic, error: err };
    }
    return { topic: topic };
};

async function sendBatch (last) {
    if (!commit.length) return;
    let current = commit[commit.length - 1].timestamp;
    if (current > last) return;

    let batch = [];
    let pick = commit.length > maxBatchSize ? maxBatchSize : commit.length;
    for (let i = 0; i < pick; i++) batch.push(commit.shift());

    let messages = [];
    batch.map(e => messages.push(e.msg));

    // console.log({ sent: pick, current: current, last: last });
    try {
        await producer.send({
            topic: topic,
            messages: messages
        });
        // console.log({ sent: messages.length });
        // console.log("Event emitted", { topic: topic, event: content.event, uid: content.uid, timestamp: content.timestamp, version: content.version });
        batch.forEach(e => e.resolve(e.index));
        return;
    } catch (err) {
        console.log(`Failed to send messages to topic ${topic}`, { error: err });
        throw err;
    }

}

const send = async function (index) {
    count++;
    let received = Date.now();
    let msg = {
        value: JSON.stringify({
            event: "index.created",
            payload: { index: index },
            meta: { test: true },
            version: "1.0.0",
            uid: uuid(),
            timestamp: Date.now()
        })
    };
    let p = new Promise(resolve => commit.push({ timestamp: received, msg: msg, resolve: resolve}));
    
    if (commit.length >= maxBatchSize) {
        sendBatch();
    } else {
        setTimeout(function () { sendBatch(received); }, 500);
    }
    
    return p;
    
};

const run = async function () {
    
    await createTopic(topic, numPartitions);
    producer = await kafka.producer({ allowAutoTopicCreation: true });
    
    await producer.connect();
    console.log("Producer connected to kafka brokers");

    let ts = Date.now();
    // for (let i = 0; i < n; i++) await send();
    for (let i = 0; i < n; i += p) {
        let ts = Date.now();
        let jobs = Array.from(Array(p),(x,index) => index + 1);
        await Promise.all(jobs.map(async (j) => await send(j)));
        let te = Date.now();
        console.log({
            "sent": p,
            "time (ms)": te-ts
        });
    }
    let te = Date.now();
    console.log({
        "total sent": count,
        "time (ms)": te-ts
    });
    
    await producer.disconnect();
    console.log("Producer disconnected");
    
};
run();