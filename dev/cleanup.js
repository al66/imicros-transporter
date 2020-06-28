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

let admin;


const topic = "service-78897099-fd18-4da5-91d4-08d9ca988fd115297b68-a3c8-444b-8c5e-0e5e9725437a";

const deleteTopic = async function (topic) {
    try {
        await admin.deleteTopics({
            topics: [topic],
            timeout: 5000          // default: 1000 (ms)
        });
        console.log("Topic deleted: " + topic);
    } catch (err) {
        console.log("Failed to delete topic " + topic);
        return { topic: topic, error: err };
    }
    return { topic: topic };
};

async function fetchTopics () {
    try {
        let meta = await admin.fetchTopicMetadata();
        console.log("Topics", meta);
        
        if (meta.topics && meta.topics.length > 0) {
            for(let i = 0; i < meta.topics.length; i++) {
                let topic = meta.topics[i];
                if (/^(service|MOL|performance)/.test(topic.name)) {
                    try {
                        await deleteTopic(topic.name);
                        console.log("Topic deleted", { topic: topic.name });
                    } catch (err) {
                        console.log("Failed to delete topic", { topic: topic.name, err });
                        throw err;
                    }
                }
                console.log("Topic checked", { topic: topic.name });
            }
        }
    } catch (err) {
        console.log("Failed to fetch meta data", err);
    }
}

const run = async function () {
    
    admin = await kafka.admin();
    await admin.connect();

    //await deleteTopic(topic);
    await fetchTopics();

    await admin.disconnect();
    console.log("Admin disconnected");
    
};
run();