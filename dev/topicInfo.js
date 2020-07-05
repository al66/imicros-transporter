const { Kafka } = require("kafkajs");
const { v4: uuid } = require("uuid");
const util = require("util");

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

async function fetchTopics () {
    try {
        let meta = await admin.fetchTopicMetadata();
        console.log("Topics", util.inspect(meta, {showHidden: false, depth: null}));
        
        /*
        if (meta.topics && meta.topics.length > 0) {
            for(let i = 0; i < meta.topics.length; i++) {
                let topic = meta.topics[i];
                console.log("Topic meta", { topic });
            }
        }
        */
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