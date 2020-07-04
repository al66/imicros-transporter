/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 *
 * for the NATS part refer to https://github.com/moleculerjs/moleculer/blob/master/src/transporters/nats.js
 *
 */
"use strict";

const BaseTransporter = require("moleculer").Transporters.Base;
const { Kafka, logLevel } = require("kafkajs");
const { BrokerDisconnectedError } = require("moleculer").Errors;
const KafkaConnect = require("./kafkaConnect");

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
        /* base options */
        super(opts);
        if (!this.opts) this.opts = {};
        if (!this.opts.nats) this.opts.nats = {};
        if (!this.opts.kafka) this.opts.kafka = {};

        /* nats sepcific options */
        this.initNats();
        
        /* common */
        this.hasBuiltInBalancer = true;
        
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

        /* kafka client */
        this.kafka = await new KafkaConnect({ transporter: this });
        
        this.logger.info("init", { nodeID: this.nodeID, maxQueueSize: this.transit.opts.maxQueueSize });
        this.broker.Promise.resolve();
    }
    
    async connect() { 
        await this.kafka.connect();
        await this.connectNats();
        /* call back transit */
        await this.onConnected();
    }
    
    async disconnect() {
        this.disconnect = true;

        await this.disconnectNats();
        await this.kafka.disconnect();
        
        this.disconnected = true;
    }
    
    makeSubscriptions(topics) {
        return this.broker.Promise.all(topics.map(({ cmd, nodeID }) => this.subscribeCommand(cmd, nodeID)));
    }    

    async unsubscribeFromBalancedCommands() {
        /* NATS */
        await new this.broker.Promise(resolve => {
            this.subscriptions.forEach(uid => this.nats.unsubscribe(uid));
            this.subscriptions = [];

            this.nats.flush(resolve);
        });
        
        /* Kafka events */
        await this.kafka.unsubscribe();
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

        if (packet.type === PACKET_EVENT && packet.target == null) {
            return this.publishBalancedEvent(packet, null);
        } else if (packet.type === PACKET_REQUEST && packet.target == null) {
            return this.publishBalancedRequest(packet);
        }

        // Other packet publishing...
        return this.publish(packet);
    }    

    publishBalancedRequest(packet) {
        const topic = `${this.prefix}.${PACKET_REQUEST}B.${packet.payload.action}`;
        const data = this.serialize(packet);

        return this.sendNats(topic, data, { packet, balanced: true });
    }

    async publishBalancedEvent(packet /* , group */) {
        const topic = `${this.prefix}.${PACKET_EVENT}B.${packet.payload.event}`;
        const data = this.serialize(packet);

        return this.kafka.send(topic, data, { packet, balanced: true, event: true });
    }
    
    publish(packet) {
        const topic = this.getTopicName(packet.type, packet.target);
        const data = this.serialize(packet);

        return this.sendNats(topic, data, { packet });
    }    
    
    async subscribeBalancedEvent(event, group) {
        let topic = `${this.prefix}.${PACKET_EVENT}B.${event}`;
        
        await this.kafka.subscribe(event, group, topic);
    }    
    
    /*---------------------------------------------------
        NATS specific methods
      ---------------------------------------------------*/

    initNats () {

        if (this.opts.nats.preserveBuffers !== false)                // Use the 'preserveBuffers' option as true as default
            this.opts.nats.preserveBuffers = true;

        if (this.opts.nats.maxReconnectAttempts == null)
            this.opts.nats.maxReconnectAttempts = -1;

        this.nats = null;

        this.subscriptions = [];
        
    }
    
    
    connectNats () {
        return new this.broker.Promise((resolve, reject) => {
            let Nats;
            try {
                Nats = require("nats");
            } catch(err) {
                /* istanbul ignore next */
                this.broker.fatal("The 'nats' package is missing! Please install it with 'npm install nats --save' command.", err, true);
            }
            const client = Nats.connect(this.opts.nats);

            client.on("connect", () => {
                this.nats = client;
                this.logger.info("NATS client is connected.");
                this.onConnected().then(resolve);
            });

           /* istanbul ignore next */
            client.on("reconnect", () => {
                this.logger.info("NATS client is reconnected.");
                this.onConnected(true);
            });

           /* istanbul ignore next */
            client.on("reconnecting", () => {
                this.logger.warn("NATS client is reconnecting...");
            });

           /* istanbul ignore next */
            client.on("disconnect", () => {
                if (this.connected) {
                    this.logger.warn("NATS client is disconnected.");
                    this.connected = false;
                }
            });

           /* istanbul ignore next */
            client.on("error", e => {
                this.logger.error("NATS error.", e.message);
                this.logger.debug(e);

                if (!client.connected)
                    reject(e);
            });

           /* istanbul ignore next */
            client.on("close", () => {
                this.connected = false;
                // Hint: It won't try reconnecting again, so we kill the process.
                this.broker.fatal("NATS connection closed.");
            });
        });        
    }
    
    disconnectNats () {
        if (this.nats) {
            this.nats.flush(() => {
                this.nats.close();
                this.nats = null;
            });
        }
    }        
    
    subscribeCommand(cmd, nodeID) {
        const t = this.getTopicName(cmd, nodeID);

        this.nats.subscribe(t, msg => this.receive(cmd, msg));

        return this.broker.Promise.resolve();
    }    

    subscribeBalancedRequest(action) {
        const topic = `${this.prefix}.${PACKET_REQUEST}B.${action}`;
        const queue = action;

        this.subscriptions.push(this.nats.subscribe(topic, { queue }, (msg) => this.receive(PACKET_REQUEST, msg)));
    }

    sendNats(topic, data) {
        /* istanbul ignore next*/
        if (!this.nats) return this.broker.Promise.resolve();

        return new this.broker.Promise(resolve => {
            this.nats.publish(topic, data, resolve);
        });
    }    
    
}
module.exports = Transporter;
