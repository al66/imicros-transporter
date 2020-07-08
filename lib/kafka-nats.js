/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 *
 * for the NATS part refer to https://github.com/moleculerjs/moleculer/blob/master/src/transporters/nats.js
 *
 */
"use strict";

const BaseTransporter = require("moleculer").Transporters.Base;
const { BrokerDisconnectedError } = require("moleculer").Errors;
const KafkaConnect = require("./kafkaConnect");
const NatsConnect = require("./natsConnect");

const PACKET_EVENT 			= "EVENT";
const PACKET_REQUEST 		= "REQ";
// const PACKET_RESPONSE		= "RES";
// const PACKET_DISCOVER 		= "DISCOVER";
const PACKET_INFO 			= "INFO";
// const PACKET_DISCONNECT 	= "DISCONNECT";
// const PACKET_HEARTBEAT 		= "HEARTBEAT";
const PACKET_PING 			= "PING";
// const PACKET_PONG 			= "PONG";


class Transporter extends BaseTransporter {

    constructor(opts) {
        /* base options */
        super(opts);
        if (!this.opts) this.opts = {};
        if (!this.opts.nats) this.opts.nats = {};
        if (!this.opts.kafka) this.opts.kafka = {};

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

        /* nats client */
        this.nats = await new NatsConnect({ transporter: this });
        
        /* kafka client */
        this.kafka = await new KafkaConnect({ transporter: this });
        
        this.logger.info("init", { nodeID: this.nodeID, maxQueueSize: this.transit.opts.maxQueueSize });
        this.broker.Promise.resolve();
    }
    
    async connect() {
        try {
            await this.nats.connect();
            await this.kafka.connect();
        } catch (err) {
            this.logger.error("Connection error", { err });
            throw err;
        }

        /* call back transit */
        return this.onConnected();
    }
    
    async disconnect() {
        await this.kafka.disconnect();
        await this.nats.disconnect();
    }
    
    makeEventSubscriptions() {
        const p = [];
        // make subscriptions for events
        const services = this.broker.getLocalNodeInfo().services;
        this.logger.info("makeSubscriptions", { services });
        services.map(service => {
                // Load-balanced/grouped events queues
            if (service.events && typeof(service.events) == "object") {
                this.logger.info("makeBalancedSubscriptions", { events: service.events });
                p.push(Object.keys(service.events).map(async (event) => {
                    const group = service.events[event].group || service.name;
                    this.logger.info("makeBalancedSubscriptions", { event, group });
                    return this.subscribeBalancedEvent(event, group);
                }));
            }
        });
        
        return this.broker.Promise.all(p);
    }
    
    makeSubscriptions(topics) {
        this.logger.info("makeSubscriptions", {topics});
        
        const p = [];
        /*
        // make subscriptions for events
        const nodeInfo = this.broker.getLocalNodeInfo();
        this.logger.info("makeSubscriptions", { nodeInfo });
        const services = this.broker.getLocalNodeInfo().services;
        this.logger.info("makeSubscriptions", { services });
        services.map(service => {
                // Load-balanced/grouped events queues
            if (service.events && typeof(service.events) == "object") {
                this.logger.info("makeBalancedSubscriptions", { events: service.events });
                p.push(Object.keys(service.events).map(async (event) => {
                    const group = service.events[event].group || service.name;
                    this.logger.info("makeBalancedSubscriptions", { event, group });
                    return this.subscribeBalancedEvent(event, group);
                }));
            }
        });
        */
        
        topics.map(({ cmd, nodeID }) => p.push(this.subscribeCommand(cmd, nodeID)));
        
        return this.broker.Promise.all(p);     
        // return this.broker.Promise.all(topics.map(({ cmd, nodeID }) => this.subscribeCommand(cmd, nodeID)));
    }    

    makeBalancedSubscriptions() {
        this.logger.info("makeBalancedSubscriptions");
        if (!this.hasBuiltInBalancer) return this.broker.Promise.resolve();

        return this.unsubscribeFromBalancedCommands().then(() => {
            const services = this.broker.getLocalNodeInfo().services;
            return this.broker.Promise.all(services.map(service => {
                const p = [];

                // Service actions queues
                if (service.actions && typeof(service.actions) == "object") {
                    p.push(Object.keys(service.actions).map(action => this.subscribeBalancedRequest(action)));
                }

                /*
                // Load-balanced/grouped events queues
                if (service.events && typeof(service.events) == "object") {
                    p.push(Object.keys(service.events).map(async (event) => {
                        const group = service.events[event].group || service.name;
                        this.logger.info("makeBalancedSubscriptions", { event, group });
                        return this.subscribeBalancedEvent(event, group);
                    }));
                }
                */

                //return this.broker.Promise.all(_.compact(flatten(p, true)));
                return this.broker.Promise.all(p);
            }));
        });
    }
    
    async unsubscribeFromBalancedCommands() {
        /* NATS */
        await this.nats.unsubscribe();
        
        /* Kafka events */
        // await this.kafka.unsubscribe();  
    }    

    /* copied from moleculer transporter base */
    /* removed logic with sending events to multiple groups - already handled by consumer groups in kafka */
    async prepublish(packet) {
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

        if (packet.type === PACKET_INFO && !this.subscriptions) {
            // const nodeInfo = this.broker.getLocalNodeInfo();
            // this.logger.info("prepublish", { nodeInfo });
            
            await this.makeEventSubscriptions();
            this.subscriptions = true;
        }
        
        // if (packet.type === PACKET_EVENT && packet.target == null) {
        if (packet.type === PACKET_EVENT) {
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

        return this.nats.send(topic, data, { packet, balanced: true });
    }

    async publishBalancedEvent(packet /* , group */) {
        const topic = `${this.prefix}.${PACKET_EVENT}B.${packet.payload.event}`;
        const data = this.serialize(packet);

        return this.kafka.send(topic, data, { packet, balanced: true, event: true });
    }
    
    publish(packet) {
        const topic = this.getTopicName(packet.type, packet.target);
        const data = this.serialize(packet);

        return this.nats.send(topic, data, { packet });
    }    
    
    async subscribeBalancedEvent(event, group) {
        let topic = `${this.prefix}.${PACKET_EVENT}B.${event}`;
        this.logger.info("subscribeBalancedEvent",{ event,group, topic});
        
        return this.kafka.subscribe(event, group, topic);
    }    
    
    subscribeCommand(cmd, nodeID) {
        const topic = this.getTopicName(cmd, nodeID);

        this.nats.subscribeCommand(cmd, nodeID, topic);

        return this.broker.Promise.resolve();
    }    

    subscribeBalancedRequest(action) {
        const topic = `${this.prefix}.${PACKET_REQUEST}B.${action}`;
        const queue = action;

        return this.nats.subscribeRequest(action, topic, queue);
    }

}
module.exports = Transporter;
