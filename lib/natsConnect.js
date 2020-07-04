/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 *
 */
"use strict";

const PACKET_REQUEST 		= "REQ";

class NatsConnect {
    
    constructor({ transporter }) {

        this.transporter = transporter;
        this.broker = transporter.broker;
        this.opts = transporter.opts.nats;
        this.logger = transporter.logger;
        this.nodeID = transporter.nodeID;
        this.prefix = transporter.prefix;
        
        if (this.opts.preserveBuffers !== false)                // Use the 'preserveBuffers' option as true as default
            this.opts.preserveBuffers = true;

        if (this.opts.maxReconnectAttempts == null)
            this.opts.maxReconnectAttempts = -1;

        this.client = null;

        this.subscriptions = [];
        
    }

    connect () {
        return new this.broker.Promise((resolve, reject) => {
            let Nats;
            try {
                Nats = require("nats");
            } catch(err) {
                /* istanbul ignore next */
                this.broker.fatal("The 'nats' package is missing! Please install it with 'npm install nats --save' command.", err, true);
            }
            const client = Nats.connect(this.opts);

            client.on("connect", () => {
                this.client = client;
                this.logger.info("NATS client is connected.");
                resolve();
            });

           /* istanbul ignore next */
            client.on("reconnect", () => {
                this.logger.info("NATS client is reconnected.");
                this.transporter.onConnected(true);
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
    
    async disconnect () {
        this.disconnecting = true;
        if (this.client) {
            this.client.on("close", () => {
                this.logger.info("NATS connection closed.");
                this.connected = false;
                this.client = null;
            });
            await new this.broker.Promise(resolve => {
                this.client.drain(resolve);
            });
        }
    }        
    
    subscribeCommand(cmd, nodeID, topic) {

        this.client.subscribe(topic, msg => this.transporter.receive(cmd, msg));

        return this.broker.Promise.resolve();
    }    

    subscribeRequest(action, topic, queue) {

        this.subscriptions.push(this.client.subscribe(topic, { queue }, (msg) => this.transporter.receive(PACKET_REQUEST, msg)));
    }

    async unsubscribe () {
        //await new this.broker.Promise(resolve => {
            this.subscriptions.forEach(async (uid) => await this.client.unsubscribe(uid));
            this.subscriptions = [];

            //this.client.flush(resolve);
        //});
    }
    
    send(topic, data) {
        /* istanbul ignore next*/
        if (!this.client || this.disconnecting) return this.broker.Promise.reject();

        return new this.broker.Promise(resolve => {
            this.client.publish(topic, data, resolve);
        });
    }    
    
}

module.exports = NatsConnect;