/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const BaseTransporter = require("moleculer").Transporters.Base;
const { Kafka, logLevel } = require("kafkajs");

const PACKET_REQUEST = "REQ";
const PACKET_EVENT = "EVENT";

class Transporter extends BaseTransporter {
    
    constructor(opts) {
        super(opts);
        if (!this.opts) this.opts = {};
        
        this.brokers = this.opts.brokers || ["localhost:9092"];
        this.maxBatchSize = this.opts.maxBatchSize || 1000;     // default 1000 messages (with awaitSend = false !)
                                                                // set awaitSend = true, when using maxBatchSize = 1
        this.timeToFlush = 50;                                  // default 50 ms
        this.awaitSend = false;                                 // default don't await
        
        this.hasBuiltInBalancer = true;
        
        this.commit = [];
        this.consumers = [];
        this.consumersBalanced = [];
    }
    
    async init(transit, messageHandler, afterConnect) {
        await super.init(transit, messageHandler, afterConnect);

        if (this.broker) {
            this.Promise = this.broker.Promise;
            this.registry = this.broker.registry;
            this.discoverer = this.broker.registry.discoverer;
            this.nodes = this.registry.nodes;

            // Disable normal HB logic
            // this.discoverer.disableHeartbeat();
        }
        
        this.logger.info("init", { nodeID: this.nodeID, maxQueueSize: this.transit.opts.maxQueueSize });
        this.broker.Promise.resolve();
    }
    
    async connect() { 
        this.logger.info("connect", { nodeID: this.nodeID, brokers: this.brokers});
        this.kafka = new Kafka({
            clientId: this.nodeID,
            brokers: this.brokers,
            logLevel: logLevel.WARN, // NOTHING, ERROR, WARN, INFO, DEBUG - default INFO
            logCreator: this.serviceLogger(),
            ssl: this.opts.ssl || null,     // refer to kafkajs documentation
            sasl: this.opts.sasl || null,   // refer to kafkajs documentation
            connectionTimeout: this.opts.connectionTimeout ||  1000,
            retry: this.opts.retry || {
                initialRetryTime: 100,
                retries: 8
                // restartOnFailure: (/*error*/) => this.broker.Promise.resolve(false)
            }
        });
    
        this.admin = await this.kafka.admin();
        await this.admin.connect();
        this.logger.info("Admin connected to kafka brokers " + this.brokers.join(","), { clientId: this.nodeID });
        this.producer = await this.kafka.producer({ allowAutoTopicCreation: true });
        await this.producer.connect();
        this.logger.info("Producer connected to kafka brokers " + this.brokers.join(","), { clientId: this.nodeID });  
        await this.onConnected();
    }
    
    async disconnect() { 
        await this.flush();
        await this.admin.disconnect();
        this.logger.info("Admin client disconnected", { clientId: this.nodeID });
        await this.producer.disconnect();
        this.logger.info("Producer disconnected", { clientId: this.nodeID });
        await Promise.all(this.consumers.map(async (consumer) => {
            try {
                await consumer.stop();
                await consumer.disconnect();
                this.logger.debug("Consumer disconnected", { clientId: this.nodeID });
            } catch (err) {
                /* istanbul ignore next */
                this.logger.error("Failed to disconnect consumer", { clientId: this.nodeID, err });
            }
        }));
        this.logger.info("Consumer disconnected", { clientId: this.nodeID });
        this.disconnected = true;
    }
    
    async send(topic, data, { balanced, packet }) { 
        let kafkaTopic = topic.replace(/\$/,"_");
        /* istanbul ignore next*/
        if (!this.producer) return this.broker.Promise.resolve();
        this.logger.debug("send called", {topic, kafkaTopic, data, balanced, packet});
        
        if (this.awaitSend) {
            await this.sendSingle(kafkaTopic, data);
        // TODO: doesn't stop in time... too much unresolved
        } else if (this.transit.opts.maxQueueSize && this.transit.pendingRequests.size >= this.transit.opts.maxQueueSize) {
            await this.sendSingle(kafkaTopic, data, true);
        } else {
            this.sendSingle(kafkaTopic, data);
        }
    }
    
    async makeSubscriptions(topics) {
        this.logger.debug("makeSubscriptions", { nodeID: this.nodeID, topics});

        let createTopics = [];
        let undefinedTopics = [];
        let definedTopics = [];
        topics.map(({ cmd, nodeID }) => {
            if (nodeID) {
                createTopics.push({
                    topic: this.getTopicName(cmd, nodeID),
                    numPartitions: this.numPartitions || 1,       // default 1
                    replicationFactor: 1                          // default 1
                });
                definedTopics.push({ cmd, nodeID });
            } else {
                undefinedTopics.push({ cmd, nodeID });
            }
        });
        if (createTopics.length) {
            this.logger.debug("Create topics ", createTopics);
            try {
                await this.admin.createTopics({
                    // validateOnly: true,  // default false
                    waitForLeaders: true,  // default true
                    timeout: 10000,          // default: 1000 (ms)
                    topics: createTopics,
                });
            } catch (err) {
                if (err.error !== "Topic with this name already exists") {
                    this.logger.error("Failed to create topics", { nodeID: this.nodeID, err });
                }
            }

            try {

                await Promise.all(definedTopics.map(async ({ cmd, nodeID }) => { 
                    let topic = this.getTopicName(cmd, nodeID);
                    let groupId = nodeID ? `${cmd}${this.nodeID}` : this.nodeID ;

                    let consumer = this.kafka.consumer({ 
                        groupId: groupId,
                        allowAutoTopicCreation: true   
                    });

                    // connect consumer and subscribe to the topic
                    await consumer.connect();
                    await consumer.subscribe({ 
                        topic: topic, 
                        fromBeginning: false 
                    });

                    // memorize consumer for cleaning up on service stop
                    this.consumers.push(consumer);

                    // start runner
                    let t = this;
                    await consumer.run({
                        eachMessage: async ({ topic, partition, message }) => {
                            this.logger.debug("received", { 
                                topic: topic, 
                                partition: partition,
                                msg: message.value.toString()
                            });
                            await t.receive(cmd, message.value.toString());
                        }
                    });

                    this.logger.debug(`Subscription for topic '${topic}' running`, { topic, groupId });
                }));


            } catch (e) {
                /* istanbul ignore next */
                this.logger.warn("Subscription for topics failed", { definedTopics });
                /* istanbul ignore next */
                throw e;
            }
        }
        
        try {

            let consumer = this.kafka.consumer({ 
                groupId: this.nodeID,
                allowAutoTopicCreation: true   
            });

            // connect consumer and subscribe to the topic
            await consumer.connect();

            await Promise.all(undefinedTopics.map(async ({ cmd, nodeID }) => { 
                let topic = this.getTopicName(cmd, nodeID);
                await consumer.subscribe({ 
                    topic: topic, 
                    fromBeginning: false 
                });
            }));

            // memorize consumer for cleaning up on service stop
            this.consumers.push(consumer);
            this.logger.debug("Consumer", {consumer });
            
            // start runner
            let t = this;
            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        let cmd = topic.replace(`${this.prefix}.`, "");
                        let packet = this.deserialize(topic, message.value.toString());
                        let payload = packet.payload;
                        this.logger.debug("received", { 
                            topic: topic, 
                            partition: partition,
                            msg: message.value.toString(),
                            cmd: cmd,
                            sender: payload.sender
                        });
                        if (payload.sender !== this.nodeID) await t.receive(cmd, message.value.toString());
                        //await t.receive(cmd, message.value.toString());
                    } catch (err) {
                        this.logger.error("Failed to process received massage", { topic, partition, message, err });
                    }
                }
            });

            this.logger.debug("Subscriptions for undefined topics running", { undefinedTopics, groupId: this.nodeID });

        } catch (e) {
            /* istanbul ignore next */
            this.logger.warn("Subscription for topics failed", { undefinedTopics });
            /* istanbul ignore next */
            throw e;
        }
        
    }
    
    async subscribeBalancedRequest(action) {
        let topic = `${this.prefix}.${PACKET_REQUEST}B.${action}`;
        let internal = /^\$/.test(action);
        let kafkaTopic = topic.replace(/\$/,"_");
        this.logger.debug("subscribeBalancedRequest", { action, nodeID: this.nodeID, topic, kafkaTopic , hasBuiltInBalancer: this.hasBuiltInBalancer });

        if (!internal) {
            // create topic first
            try {
                let createTopics = [];
                createTopics.push({
                    topic: kafkaTopic,
                    numPartitions: this.numPartitions || 5,       // default 1
                    replicationFactor: 1                          // default 1
                });
                await this.admin.createTopics({
                    // validateOnly: true,  // default false
                    waitForLeaders: true,  // default true
                    timeout: 10000,          // default: 1000 (ms)
                    topics: createTopics,
                });
            } catch (err) {
                if (err.error !== "Topic with this name already exists") {
                    this.logger.error("Failed to create topics", { nodeID: this.nodeID, err });
                }
            }
        }
    
        try {

            let consumer = this.kafka.consumer({ 
                groupId: `${this.nodeID}${action}`,
                allowAutoTopicCreation: true   
            });

            // memorize consumer for cleaning up on service stop
            this.consumersBalanced.push(consumer);

            // connect consumer and subscribe to the topic
            await consumer.connect();
            await consumer.subscribe({ 
                topic: kafkaTopic, 
                fromBeginning: false 
            });

            // start runner
            let t = this;
            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    this.logger.debug("received", { 
                        topic: topic, 
                        partition: partition,
                        msg: message.value.toString()
                    });
                    await t.receive(PACKET_REQUEST, message.value.toString());
                }
            });

            this.logger.debug("Subscription for topic running", { kafkaTopic });

        } catch (e) {
            /* istanbul ignore next */
            this.logger.warn("Subscription for topic failed", { kafkaTopic });
            /* istanbul ignore next */
            throw e;
        }        

    }
    
    async subscribeBalancedEvent(event, group) {
        let subEvent = /\*/.test(event) ? new RegExp(event) : event; 
        let topic = `${this.prefix}.${PACKET_EVENT}B.${group}.${event}`;
        let kafkaTopic = topic.replace(/\$/,"_");
        kafkaTopic = /\*/.test(kafkaTopic) ? new RegExp(kafkaTopic) : kafkaTopic; 
        
        this.logger.info("subscribeBalancedRequest", { event, group, nodeID: this.nodeID, topic, kafkaTopic , hasBuiltInBalancer: this.hasBuiltInBalancer });

        // create topic first
        if (subEvent === event) {
            
            try {
                let createTopics = [];
                createTopics.push({
                    topic: kafkaTopic,
                    numPartitions: this.numPartitions || 5,       // default 1
                    replicationFactor: 1                          // default 1
                });
                await this.admin.createTopics({
                    // validateOnly: true,   // default false
                    waitForLeaders: true,    // default true
                    timeout: 10000,          // default: 1000 (ms)
                    topics: createTopics,
                });
            } catch (err) {
                if (err.error !== "Topic with this name already exists") {
                    this.logger.error("Failed to create topics", { nodeID: this.nodeID, err });
                }
            }
    
        }
        
        try {

            let consumer = this.kafka.consumer({ 
                groupId: `${group}`,
                allowAutoTopicCreation: true   
            });

            // memorize consumer for cleaning up on service stop
            this.consumersBalanced.push(consumer);

            // connect consumer and subscribe to the topic
            await consumer.connect();
            await consumer.subscribe({ 
                topic: kafkaTopic, 
                fromBeginning: false 
            });

            // start runner
            let t = this;
            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    this.logger.debug("received", { 
                        topic: topic, 
                        partition: partition,
                        msg: message.value.toString()
                    });
                    await t.receive(PACKET_EVENT, message.value.toString());
                }
            });

            this.logger.info("Subscription for topic running", { kafkaTopic });

        } catch (e) {
            /* istanbul ignore next */
            this.logger.warn("Subscription for topic failed", { kafkaTopic });
            /* istanbul ignore next */
            throw e;
        }        

    }
    
    async unsubscribeFromBalancedCommands() {
        await Promise.all(this.consumersBalanced.map(async (consumer) => {
            try {
                await consumer.stop();
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
    
    
    async sendBatch (last) {
        if (!this.commit.length) return;
        let current = this.commit[this.commit.length - 1].timestamp;
        if (current > last) return;

        let batch = [];
        let pick = this.commit.length > this.maxBatchSize ? this.maxBatchSize : this.commit.length;
        for (let i = 0; i < pick; i++) batch.push(this.commit.shift());
        let topicMessages = [];
        // TODO: group by topics first...
        batch.map(e => topicMessages.push({ topic: e.topic, messages: [{ value: e.msg }]}));
        try {
            await this.producer.sendBatch({ topicMessages, acks: 0 });
            this.logger.debug("batch sent",{ sent: pick, current: current, last: last });
            batch.forEach(e => e.resolve(e.index));
            return;
        } catch (err) {
            this.logger.error(`Failed to send messages to topic ${this.topic}`, { error: err });
            throw err;
        }

    }

    async flush () {
        await this.sendBatch();
        this.logger.debug("send queue", { commit: this.commit });
    }
    
    async sendSingle (topic, data, flush) {
        let received = Date.now();
        let p = new Promise(resolve => this.commit.push({ timestamp: received, topic: topic, msg: data, resolve: resolve}));

        if (flush) {
            await this.sendBatch();
        } else if (this.commit.length >= this.maxBatchSize) {
            this.sendBatch();
        } else {
            setTimeout(function () { this.sendBatch(received); }.bind(this), this.timeToFlush);
        }
        return p;
    }

    serviceLogger() {
        let t = this;
        return () => ({ level, log }) => {
            if (t.disconnected) return;
            let namespace = "kafkajs:";
            switch(level) {
                /* istanbul ignore next */
                case logLevel.ERROR:
                    return this.logger.error(namespace + log.message, log);
                /* istanbul ignore next */
                case logLevel.WARN:
                    return this.logger.warn(namespace + log.message, log);
                /* istanbul ignore next */
                case logLevel.INFO:
                    return this.logger.info(namespace + log.message, log);
                /* istanbul ignore next */
                case logLevel.DEBUG:
                    return this.logger.debug(namespace + log.message, log);
                /* istanbul ignore next */
                case logLevel.NOTHING:
                    return this.logger.debug(namespace + log.message, log);
            }
        };
    }
}

module.exports = Transporter;
