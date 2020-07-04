/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const BaseTransporter = require("moleculer").Transporters.Base;
const { Kafka, logLevel } = require("kafkajs");
const { BrokerDisconnectedError } = require("moleculer").Errors;
const Redis = require("ioredis");

const PACKET_EVENT 			= "EVENT";
const PACKET_REQUEST 		= "REQ";
const PACKET_RESPONSE		= "RES";
const PACKET_DISCOVER 		= "DISCOVER";
const PACKET_INFO 			= "INFO";
const PACKET_DISCONNECT 	= "DISCONNECT";
const PACKET_HEARTBEAT 		= "HEARTBEAT";
const PACKET_PING 			= "PING";
const PACKET_PONG 			= "PONG";

class Transporter extends BaseTransporter {
    
    constructor(opts) {
        super(opts);
        if (!this.opts) this.opts = {};
        
        this.redisOptions = this.opts.redis || {};   // w/o settings the client uses defaults: 127.0.0.1:6379
        
        this.brokers = this.opts.brokers || ["localhost:9092"];
        this.maxBatchSize = this.opts.maxBatchSize || 10000;     // default 1000 messages (with awaitSend = false !)
                                                                 // set awaitSend = true, when using maxBatchSize = 1
        this.timeToFlush = 500;                                  // default 50 ms
        this.awaitSend = false;                                  // default don't await
        
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
        
        await this.connectRedis();

        // listen to registration events on redis streams
        await this.listenToRegistryEvents();
        this.logger.info("registry listener running");
        
        
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
        this.disconnect = true;
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

        await this.disconnectRedis();
        
    }
    
    async getTopics () {
        let existing = [];
        let meta = await this.admin.fetchTopicMetadata();
        if (meta.topics && meta.topics.length) {
            meta.topics.map(e => existing.push(e.name));
        }
        this.topics = existing;
        this.logger.debug("Topics", this.topics);
        return existing;
    }
    
    /* Finished */
    async checkTopics({ topics }) {
        this.logger.debug("Check topics", { topics });
        
        let existing = await this.getTopics();
        
        let newTopics = [];
        topics.map(topic => {
            if (!existing.includes(topic)) newTopics.push({
                topic: topic,
                numPartitions: this.numPartitions || 10,       // default 10
                replicationFactor: 1                           // default 1
            });
        });
        this.logger.debug("New topics", { newTopics });
        
        if (newTopics.length > 0) {
            try {
                await this.admin.createTopics({
                    // validateOnly: true,  // default false
                    waitForLeaders: true,  // default true
                    timeout: 10000,          // default: 1000 (ms)
                    topics: newTopics,
                });
                // update topics member variable
                await this.getTopics();
            } catch (err) {
                if (err.error !== "Topic with this name already exists") {
                    this.logger.error("Failed to create topics", { nodeID: this.nodeID, err });
                }
            }
        }
    }
    
    connectRedis () {
        return new Promise(((resolve, reject) => {

            this.redis = new Redis(this.redisOptions);

            this.redis.on("connect", (() => {
                this.redisConnected = true;
                this.logger.info("Connected to Redis");
                resolve();
            }).bind(this));

            this.redis.on("close", (() => {
                this.redisConnected = false;
                this.logger.info("Disconnected from Redis");
            }).bind(this));

            /* istanbul ignore next */
            this.redis.on("error", ((err) => {
                this.logger.error("Redis redis error", err.message);
                this.logger.debug(err);
                /* istanbul ignore else */
                if (!this.redisConnected) reject(err);
            }).bind(this));
        }).bind(this));
    }        

    async disconnectRedis () {
        
        // set stopped for use in methods
        this.redisStopped = true;
        // break runner loop
        if (this.redisRunner) clearTimeout(this.redisRunner);
        // disconnect client
        return new Promise(((resolve) => {
            /* istanbul ignore else */
            if (this.redis && this.redisConnected) {
                this.redis.on("close", (() => {
                    this.redisConnected = false;
                    resolve();
                }).bind(this));
                this.redis.disconnect();
            } else {
                resolve();
            }
        }).bind(this));
    }

    async listenToRegistryEvents() {
 
        this.streamIDs = [0,0,0,0,0,0];
        let init = true;
        const read = async function read () {
            if (this.redisStopped || !this.redisConnected) return this.redisRunner ? clearTimeout(this.redisRunner) : true ;

            try {
                
                this.logger.debug("get next message from stream", { streamIDs: this.streamIDs });
                let streams = [PACKET_DISCOVER, PACKET_INFO, PACKET_DISCONNECT, PACKET_HEARTBEAT, PACKET_PING, PACKET_PONG];
                // let result = await t.redis.xread("BLOCK",1000,"COUNT",10000,"STREAMS",streams, this.streamIDs); 
                let result = await this.redis.xread("BLOCK",100,"STREAMS",streams, this.streamIDs); 

                if (Array.isArray(result)) {
                    let messages = [];
                    // stream
                    for (let s = 0; s<result.length; s++) {
                        let stream = result[s][0];
                        // array of messages
                        let a = result[s][1];
                        for (let m = 0; m<a.length; m++) {
                            let message = {
                                stream: stream,
                                id: a[m][0]
                            };
                            let fields = a[m][1];
                            for (let f = 0; f<fields.length; f+=2 ) {
                                if ( fields[f] === "message" ) {
                                    try {
                                        let value = fields[f+1];
                                        message[fields[f]] = value;

                                    } catch (e) {
                                        /* could not happen if created with this service */ 
                                        /* istanbul ignore next */ 
                                        {
                                            this.logger.error("Failed parsing message", { stream: stream, id: a[m][0] });
                                            message[fields[f]] = fields[f+1];
                                        }
                                    }   
                                } else {
                                    message[fields[f]] = fields[f+1];
                                }
                            }
                            messages.push(message);
                        }
                        let index = [PACKET_DISCOVER, PACKET_INFO, PACKET_DISCONNECT, PACKET_HEARTBEAT, PACKET_PING, PACKET_PONG].indexOf(stream);
                        this.streamIDs[index] = a[a.length-1][0];
                    }
                    if (!init) {
                        await Promise.all(messages.map(async (message) => {
                            this.logger.debug("received", { message });
                            if (!message.stream || !message.message) return;

                            let packet = this.deserialize(message.stream, message.message.toString());
                            let payload = packet.payload;
                            this.logger.debug("received", { message , packet });
                            // ignore messages of first loop, ignore own messages, ignore messages for other nodes
                            if (payload.sender !== this.nodeID || ( packet.target && packet.target === this.nodeID))  await this.receive(message.stream, message.message);
                        }));
                    }
                }
                init = false;
            } catch (err) {
                this.logger.info("read from redis failed", { err });
            }
        }.bind(this);   
        
        // start runner
        this.redisRunner = setInterval(async () => await read(), 1000);  
    }

    async publishRegistryEvent({ packet }) {
        if (!this.redisConnected) return this.logger.error("Redis not connected - publish registry event failed", { packet });

        const data = this.serialize(packet).toString();

        let id;
        try {
            this.logger.debug("publish registry event - call xadd", { packet, data });
            await this.redis.xadd(packet.type,"MAXLEN","~",10000,"*","message", data).timeout(5000);
            // this.redis.xadd(packet.type,"MAXLEN","~",10000,"*","message", data);
            // id = await this.redis.xadd(packet.type,"MAXLEN","~",10000,"*","message", data).timeout(5000);
            // this.logger.debug("redis id", { packet, id });
        } catch (err) {
            this.logger.warn("Failed to send registry event", { packet, error: err.message });
        }
        
        this.logger.debug("published registry event", {data, packet, id});
        
        return;
    }

    async makeSubscriptions(topics) {
        this.logger.debug("makeSubscriptions", { nodeID: this.nodeID, topics});

        // add all node specific topics as common topics -> refer to transit.js method makeSubscriptions 
        topics.push({ cmd: PACKET_EVENT });
        topics.push({ cmd: PACKET_REQUEST });
        topics.push({ cmd: `${PACKET_REQUEST}B` });
        topics.push({ cmd: PACKET_RESPONSE });
        
        let checkTopics = [];
        let undefinedTopics = [];
        topics.map(({ cmd, nodeID }) => {
            // ignore registry commands
            if ([PACKET_DISCOVER, PACKET_INFO, PACKET_DISCONNECT, PACKET_HEARTBEAT, PACKET_PING, PACKET_PONG].includes(cmd)) return;
            // ignore all targeting commands
            if (nodeID) return;
            // balanced actions
            if (cmd === `${PACKET_REQUEST}B`) {
                checkTopics.push(this.getTopicName(cmd, nodeID));
            } else {
                checkTopics.push(this.getTopicName(cmd, nodeID));
                undefinedTopics.push({ cmd, nodeID });
            }
        });
        
        // create missing topics first
        await this.checkTopics({ topics: checkTopics });

        try {

            let consumer = this.kafka.consumer({ 
                groupId: this.nodeID,
                allowAutoTopicCreation: false   
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
                        // ignore own messages, ignore messages for other nodes
                        if (payload.sender !== this.nodeID || ( packet.target && packet.target !== this.nodeID) )  await t.receive(cmd, message.value.toString());
                        //await t.receive(cmd, message.value.toString());
                    } catch (err) {
                        this.logger.error("Failed to process received massage", { topic, partition, message, err });
                    }
                }
            });

            this.logger.info("Subscriptions for undefined topics running", { undefinedTopics, groupId: this.nodeID });

        } catch (e) {
            /* istanbul ignore next */
            this.logger.warn("Subscription for topics failed", { undefinedTopics });
            /* istanbul ignore next */
            throw e;
        }
        
    }
    
    async subscribeBalancedRequest(action) {
        // let topic = `${this.prefix}.${PACKET_REQUEST}B.${action}`;
        let topic = `${this.prefix}.${PACKET_REQUEST}B`;
        let internal = /^\$/.test(action);
        let kafkaTopic = topic.replace(/\$/,"_");
        this.logger.debug("subscribeBalancedRequest", { action, nodeID: this.nodeID, topic, kafkaTopic , hasBuiltInBalancer: this.hasBuiltInBalancer });

        // create missing topics first
        await this.checkTopics({ topics: [ kafkaTopic ] });
    
        try {

            let consumer = this.kafka.consumer({ 
                groupId: `${action}`,
                allowAutoTopicCreation: false   
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
                    let packet = this.deserialize(topic, message.value.toString());
                    let payload = packet.payload;

                    this.logger.debug("received", { 
                        topic: topic, 
                        partition: partition,
                        msg: message.value.toString(),
                        packet,
                        payload,
                        action
                    });
                    // ignore own messages, ignore messages not for this action
                    if ( payload.action === action )  await t.receive(PACKET_REQUEST, message.value.toString());
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

        let topic = `${this.prefix}.${PACKET_EVENT}B.${event}`;
        let kafkaTopic = topic.replace(/\$/,"_");
        kafkaTopic = /\*/.test(kafkaTopic) ? new RegExp(kafkaTopic) : kafkaTopic; 
        
        this.logger.info("subscribeBalancedRequest", { event, group, nodeID: this.nodeID, topic, kafkaTopic , hasBuiltInBalancer: this.hasBuiltInBalancer });

        // create missing topics first
        if (subEvent === event) {
            await this.checkTopics({ topics: [ kafkaTopic ] });
        }
            
        try {

            let consumer = this.kafka.consumer({ 
                groupId: `${group}`,
                allowAutoTopicCreation: false   
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
            consumer.run({
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

    /* copied from moleculer transporter base */
    /* changed topic determination (w/o action)*/
    publishBalancedRequest(packet) {
        const topic = `${this.prefix}.${PACKET_REQUEST}B`;
        const data = this.serialize(packet);

        return this.send(topic, data, { packet, balanced: true });
    }   
    
    publish(packet) {
        const topic = this.getTopicName(packet.type, null);
        const data = this.serialize(packet);

        return this.send(topic, data, { packet });
    }
    
    /* copied from moleculer transporter base */
    /* removed logic with sending events to multiple groups - already handled by consumer groups in kafka */
    prepublish(packet) {
        this.logger.debug("prepublish", { packet });

        // Safely handle disconnected state
        if (!this.connected) {
           // For packets that are triggered intentionally by users, throw a retryable error.
            if ([PACKET_REQUEST, PACKET_EVENT, PACKET_PING].includes(packet.type)) {
                return this.broker.Promise.reject(new BrokerDisconnectedError());
            }

           // For internal packets like INFO and HEARTBEATS, skip sending and don't throw
            else {
                return this.broker.Promise.resolve();
            }
        }

        // All registry events
        if ([PACKET_DISCOVER,PACKET_INFO,PACKET_DISCONNECT,PACKET_HEARTBEAT,PACKET_PING,PACKET_PONG].includes(packet.type))
            return this.publishRegistryEvent({ packet });        
        
        
        if (packet.type === PACKET_EVENT && packet.target == null) {
            return this.publishBalancedEvent(packet, null);
        } else if (packet.type === PACKET_REQUEST && packet.target == null) {
            return this.publishBalancedRequest(packet);
        }

        // Other packet publishing...
        return this.publish(packet);
    }    

    getTopicName(cmd /*, nodeID */) {
        // ignore nodeID -> useage of common topics 
        // return this.prefix + "." + cmd + (nodeID ? "." + nodeID : "");
        return this.prefix + "." + cmd;
    }    

    async publishBalancedEvent(packet /* , group */) {
        const topic = `${this.prefix}.${PACKET_EVENT}B.${packet.payload.event}`;
        const data = this.serialize(packet);

        // to avoid any errors whith non-existing topics...
        if (!this.topics.includes(topic)) await this.checkTopics({ topics: [ topic ] });
        
        return this.send(topic, data, { packet, balanced: true, event: true });
    }    

    async send(topic, data, { balanced, packet }) { 
        /* istanbul ignore next*/
        if (!this.producer) return this.broker.Promise.resolve();

        let kafkaTopic = topic.replace(/\$/,"_");       // $ of internal services not allowed as topic names
        let received = Date.now();
        let p = new Promise(resolve => this.commit.push({ timestamp: received, topic: kafkaTopic, msg: data, resolve: resolve}));

        if (this.commit.length >= this.maxBatchSize) {
            this.sendBatch();
        } else {
            setTimeout(function () { this.sendBatch({ last: received }); }.bind(this), this.timeToFlush);
        }
        this.logger.debug("send packet queued", {topic, kafkaTopic, data, balanced, packet});
        return p;
    }

    async sendBatch ({ last, flush = false } = {}) {
        if (!this.commit.length) return;
        let now = Date.now();
        let current = this.commit[this.commit.length - 1].timestamp;
        // if (last && current > last && (now - last) < this.timeToFlush) return;
        if (!flush && (last && current > last && (now - last) < this.timeToFlush)) return;

        let batch = [];
        let pick = this.commit.length > this.maxBatchSize ? this.maxBatchSize : this.commit.length;
        for (let i = 0; i < pick; i++) batch.push(this.commit.shift());
        let topicMessages = [];
        // TODO: group by topics first...
        batch.map(e => topicMessages.push({ topic: e.topic, messages: [{ value: e.msg }]}));
        try {
            // await this.producer.sendBatch({ topicMessages, acks: -1 });
            await this.producer.sendBatch({ topicMessages });
            this.logger.debug("batch sent",{ sent: pick, current, last, now });
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
    
    serviceLogger() {
        return () => (({ level, log }) => {
            if (this.disconnected) return;
            let namespace = "kafkajs:";
            switch(level) {
                /* istanbul ignore next */
                case logLevel.ERROR:
                    // downgrade the rebalance errors as they are 'normal' business
                    if (log.error === "The group is rebalancing, so a rejoin is needed") return this.logger.debug(namespace + log.message, log);
                    // downgrade after start disconnecting
                    if (log.error === "The coordinator is not aware of this member" && this.disconnect) return this.logger.debug(namespace + log.message, log);
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
        }).bind(this);
    }
        
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

}

module.exports = Transporter;
