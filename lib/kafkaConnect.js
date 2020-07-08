/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 *
 */
"use strict";

const PACKET_EVENT 			= "EVENT";

class KafkaConnect {
    
    constructor({ transporter }) {

        this.transporter = transporter;
        this.broker = transporter.broker;
        this.opts = transporter.opts.kafka;
        this.logger = transporter.logger;
        this.nodeID = transporter.nodeID;
        this.prefix = transporter.prefix;
        
        this.brokers = this.opts.brokers || ["localhost:9092"];
        
        /* max batch size and time to flush have an important effect on the perfomance */
        /* if the average messages larger then hundred bytes, it could be better to reduce the max batch size */
        this.maxBatchSize = this.opts.maxBatchSize || 2000;       // default 2000 messages
        this.timeToFlush = 50;                                    // default 50 ms

        this.topics = [];
        this.requestedTopics = [];
        
        this.commit = [];
        this.consumers = [];
        this.consumersBalanced = [];    
        
        this.timers = [];
        
    }
    
    async connect () {
        
        try {
            const { Kafka, logLevel } = require("kafkajs");
            this.Kafka = Kafka;
            this.logLevel = logLevel;
        } catch(err) {
            /* istanbul ignore next */
            this.broker.fatal("The 'kafkajs' package is missing! Please install it with 'npm install kafkajs --save' command.", err, true);
        }

        this.logger.info("connect", { nodeID: this.nodeID, brokers: this.brokers});
        this.client = new this.Kafka({
            clientId: this.nodeID,
            brokers: this.brokers,
            logLevel: this.logLevel.WARN, // NOTHING, ERROR, WARN, INFO, DEBUG - default INFO
            logCreator: this.serviceLogger(),
            ssl: this.opts.ssl || null,     // refer to kafkajs documentation
            sasl: this.opts.sasl || null,   // refer to kafkajs documentation
            connectionTimeout: this.opts.connectionTimeout ||  1000,
            retry: this.opts.retry || {
                initialRetryTime: 100,
                retries: 8                      // default 8
                // restartOnFailure: (error) => this.broker.Promise.resolve(false)
            }
        });
    
        this.admin = await this.client.admin();
        await this.admin.connect();
        this.logger.info("Admin connected to kafka brokers " + this.brokers.join(","), { clientId: this.nodeID });
        // get all currently existing topics 
        this.getTopics();
        
        this.producer = await this.client.producer({ allowAutoTopicCreation: false });
        await this.producer.connect();
        this.logger.info("Producer connected to kafka brokers " + this.brokers.join(","), { clientId: this.nodeID });  
        
        this.startBatchRunner();
        
        this.connected = true;
    }
    
    async disconnect() {
        this.disconnecting = true;
        
        await this.flush();
        
        await this.admin.disconnect();
        this.logger.info("Admin client disconnected", { clientId: this.nodeID });
        await this.producer.disconnect();
        this.logger.info("Producer disconnected", { clientId: this.nodeID });
        await Promise.all(this.consumers.map(async (consumer) => {
            try {
                // await consumer.stop();
                await consumer.disconnect();
                this.logger.debug("Consumer disconnected", { clientId: this.nodeID });
            } catch (err) {
                /* istanbul ignore next */
                this.logger.error("Failed to disconnect consumer", { clientId: this.nodeID, err });
            }
        }));
        this.logger.info("Consumer disconnected", { clientId: this.nodeID });

        await Promise.all(this.consumersBalanced.map(async (consumer) => {
            try {
                // await consumer.stop();
                await consumer.disconnect();
                this.logger.debug("Balanced consumer disconnected", { clientId: this.nodeID });
            } catch (err) {
                /* istanbul ignore next */
                this.logger.error("Failed to disconnect balanced consumer", { clientId: this.nodeID, err });
            }
        }));
        this.logger.info("Balanced consumer disconnected", { clientId: this.nodeID });

        this.disconnected = true;
    }

    subscribe(event, group, topic) {
        return new Promise(async (resolve, reject) => {
            
            let subEvent = /\*/.test(event) ? new RegExp(event) : event; 

            let kafkaTopic = topic.replace(/\$/,"_");
            kafkaTopic = /\*/.test(kafkaTopic) ? new RegExp(kafkaTopic) : kafkaTopic; 

            this.logger.info("subscribeBalancedRequest", { event, group, nodeID: this.nodeID, topic, kafkaTopic });

            // create missing topics first
            if (subEvent === event && !this.topics.includes(kafkaTopic)) {
                await this.waitForTopic({ topic: kafkaTopic });
            }

            try {

                let consumer = this.client.consumer({ 
                    groupId: `${group}`,
                    allowAutoTopicCreation: false   
                });

                // memorize consumer for cleaning up on service stop
                this.consumersBalanced.push(consumer);

                // connect consumer and subscribe to the topic
                await consumer.connect();
                await consumer.subscribe({ 
                    topic: kafkaTopic, 
                    fromBeginning: true 
                });

                // start runner
                consumer.run({
                    eachMessage: async ({ topic, partition, message }) => {
                        this.logger.info("received", { 
                            topic: topic, 
                            partition: partition,
                            msg: message.value.toString()
                        });
                        return new Promise(((resolve) => {
                            setImmediate(async () => {
                                // inject group
                                let packet = this.transporter.deserialize(PACKET_EVENT, message.value.toString());
                                if ( packet.payload ) {
                                    if (packet.payload.groups && Array.isArray(packet.payload.groups)) {
                                        if (!packet.payload.groups.includes(group)) packet.payload.groups.push(group);
                                    } else {
                                        packet.payload.groups = [group];
                                    }
                                }
                                // packet.payload.groups ? packet.payload.groups.push(group) : packet.payload.groups = [group];
                                const data = this.transporter.serialize(packet);
                                // process message
                                await this.transporter.receive(PACKET_EVENT, data);
                                this.logger.debug("processed", { 
                                    topic: topic, 
                                    partition: partition,
                                    msg: message.value.toString(),
                                    packet,
                                    data: data.toString(),
                                    groups: packet.groups
                                });
                                resolve();
                            });
                        }).bind(this));
                    }
                });

                this.logger.info("Subscription for topic running", { kafkaTopic });
                resolve();

            } catch (e) {
                /* istanbul ignore next */
                this.logger.error("Subscription for topic failed", { kafkaTopic });
                reject(e);
            }        

        });
        
    }        
    async getTopics () {
        try {
            let existing = [];
            let meta = await this.admin.fetchTopicMetadata();
            if (meta.topics && meta.topics.length) {
                meta.topics.map(e => existing.push(e.name));
            }
            this.topics = existing;
            this.logger.debug("Existing Topics", this.topics);
            return existing;
        } catch (err) {
            this.logger.error("Failed to retrieve topics from kafka", { err });
        }
    }
    
    /* Finished */
    checkTopics({ topics }) {
        this.logger.debug("Check topics", { topics });

        topics.map(topic => {
            if (!this.topics.includes(topic) && !this.requestedTopics.includes(topic)) {
                this.createTopic({ topic });
            }
        });
        
        this.getTopics();
    }
    
    createTopic ({ topic }) {
        if (this.requestedTopics.includes(topic)) return;
        
        this.requestedTopics.push(topic);
        return new Promise(async (resolve, reject) => {
            let newTopics = [{
                topic: topic,
                numPartitions: this.numPartitions || 20,       // default 10
                replicationFactor: 1                           // default 1
            }];
            this.logger.debug("Create new topic", { newTopics });
            try {
                await this.admin.createTopics({
                    // validateOnly: true,  // default false
                    waitForLeaders: true,  // default true
                    timeout: 30000,          // default: 1000 (ms)
                    topics: newTopics,
                });
                this.requestedTopics.splice(this.requestedTopics.indexOf(topic.topic),1);
                this.logger.info("New topics created", { newTopics, requested: this.requestedTopics });
                resolve();
            } catch (err) {
                if (err.error !== "Topic with this name already exists") {
                    this.logger.error("Failed to create topics", { nodeID: this.nodeID, err });
                    reject(err);
                }
                resolve();
            }
        }); 
    }
    
    waitForTopic({ topic }) {
        return new Promise((resolve) => {
            let timer;
            let t = this;
            async function check() {
                if (!t.topics.includes(topic)) {
                    if (!t.requestedTopics.includes(topic)) {
                        try {
                            await t.createTopic ({ topic });
                        } catch (err) {
                            // reject(err); // try again ... TODO: Timeout or max retries
                        }
                    }
                    t.logger.info("Waiting for topic", { topic });
                    t.getTopics();
                } else {
                    clearInterval(timer);
                    resolve();
                }
            }
            timer = setInterval(check,1000);
        });
        
    }
    
    async send(topic, data, { balanced, packet }) { 
        /* istanbul ignore next*/
        if (!this.producer || this.disconnecting ) {
            this.logger.warn("not yet available",{ producer: !this.producer, disconnecting: this.disconnecting });
            return this.broker.Promise.reject();
        }

        let kafkaTopic = topic.replace(/\$/,"_");       // $ of internal services not allowed as topic names

        // to avoid any errors whith non-existing topics...
        if (!this.topics.includes(kafkaTopic)) {
            if (!this.requestedTopics.includes(kafkaTopic)) {
                this.checkTopics({ topics: [ kafkaTopic ] });
            } 
        }
        
        let received = Date.now();
        let p = new Promise((resolve,reject) => this.commit.push({ timestamp: received, topic: kafkaTopic, msg: data, resolve, reject }));
        
        this.logger.debug("send packet queued", {topic, kafkaTopic, data, balanced, packet});
        return p;
    }

    async startBatchRunner () {
        // start runner
        this.batchRunner = setInterval(async () => await this.sendBatch(), this.timeToFlush);  
    }
    
    async sendBatch ({ flush = false } = {}) {
        if (!this.producer || this.disconnecting || this.disconnected) {
            this.commit.forEach(e => e.reject(e.index));
            return this.batchRunner ? clearTimeout(this.batchRunner) : null ;
        }
        
        // nothing to do
        if (!this.commit.length) return;
        
        // not yet full, can it wait for next cyycle ?
        if (this.commit.length < this.maxBatchSize && !flush) {
            let oldest = this.commit[0].timestamp;
            let now = Date.now();
            if ((now - oldest) < this.timeToFlush) return;
        }

        // start sending
        let batch = [];
        let pick = ( this.commit.length + batch.length ) > this.maxBatchSize ? this.maxBatchSize : ( this.commit.length + batch.length );

        // final check
        let missing = [];
        for (let i = 0; i < pick; i++) {
            let topic = this.commit[i].topic;
            if (!this.topics.includes(topic) && !missing.includes(topic)) missing.push(topic);
        }
        if (missing.length > 0) {
            this.logger.info("Missing topics - wait for next loop", { missing });
            this.checkTopics({ topics: missing }); 
            return;
        }
        
        // start picking
        for (let i = 0; i < pick; i++) {
            batch.push(this.commit.shift());
        }
            
        // nothing picked
        if (!batch.length) return;
        
        let topicMessages = [];
        // TODO: group by topics first...
        batch.map(e => topicMessages.push({ topic: e.topic, messages: [{ value: e.msg }]}));
        
        
        try {
            // await this.producer.sendBatch({ topicMessages, acks: -1 });
            await this.producer.sendBatch({ topicMessages });
            this.logger.info("batch sent",{ sent: pick });
            batch.forEach(e => e.resolve(e.index));
            return;
        } catch (err) {
            this.logger.error("Failed to send batch of queued messages", { topicMessages, err });
            throw err;
        }

    }

    async flush () {
        await this.sendBatch({ flush: true });
        this.logger.debug("send queue flushed", { commit: this.commit });
    }

    async unsubscribe () {
        if (!this.connected) return;
        await Promise.all(this.consumersBalanced.map(async (consumer) => {
            try {
                // await consumer.stop();
                await consumer.disconnect();
                this.logger.debug("Balanced consumer disconnected", { clientId: this.nodeID });
            } catch (err) {
                /* istanbul ignore next */
                this.logger.error("Failed to disconnect balanced consumer", { clientId: this.nodeID, err });
            }
        }));
        this.logger.info("Balanced consumer disconnected", { clientId: this.nodeID });
        this.broker.Promise.resolve();
    }
    
    serviceLogger() {
        return () => (({ level, log }) => {
            if (this.disconnected) return;
            let namespace = "kafkajs:";
            switch(level) {
                /* istanbul ignore next */
                case this.logLevel.ERROR:
                    // downgrade the rebalance errors as they are 'normal' business
                    if (log.error === "The group is rebalancing, so a rejoin is needed") return this.logger.debug(namespace + log.message, log);
                    // downgrade after start disconnecting
                    if (log.error === "The coordinator is not aware of this member" && this.disconnect) return this.logger.debug(namespace + log.message, log);
                    return this.logger.error(namespace + log.message, log);
                /* istanbul ignore next */
                case this.logLevel.WARN:
                    return this.logger.warn(namespace + log.message, log);
                /* istanbul ignore next */
                case this.logLevel.INFO:
                    return this.logger.info(namespace + log.message, log);
                /* istanbul ignore next */
                case this.logLevel.DEBUG:
                    return this.logger.debug(namespace + log.message, log);
                /* istanbul ignore next */
                case this.logLevel.NOTHING:
                    return this.logger.debug(namespace + log.message, log);
            }
        }).bind(this);
    }
        
}

module.exports = KafkaConnect;

