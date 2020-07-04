"use strict";
let Promise = require("bluebird");
const { v4: uuid } = require("uuid");
const wtf = require("wtfnode");

const { ServiceBroker } = require("moleculer");
const Transporter = require("../lib/kafka-nats");

process.env.REDIS_HOST = "192.168.2.124";
process.env.REDIS_PORT = 6379;
process.env.REDIS_AUTH = "";
process.env.REDIS_DB = 0;

const transporterSettings = {
    kafka: {
        brokers: ["192.168.2.124:9092"]
    },
    nats: {
        url: "nats://192.168.2.124:4222"
        // user: "admin",
        //pass: "1234"
    },
    redis: {
        port: process.env.REDIS_PORT || 6379,
        host: process.env.REDIS_HOST || "127.0.0.1",
        password: process.env.REDIS_AUTH || "",
        db: process.env.REDIS_DB || 0,
    }
};


// KAFKA
let kafka1;
let kafka2;
let kafka3;
// let kafka4;
(async function () {

    const n = 1000000;
    const p = 20000;
    
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true });

    kafka2 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true });
    kafka2.createService({
        name: "math",
        actions: {
            add(ctx) {
                this.logger.debug("math.add", { nodeID: this.broker.nodeID });
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    kafka3 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true });
    kafka3.createService({
        name: "math",
        actions: {
            add(ctx) {
                this.logger.debug("math.add", { nodeID: this.broker.nodeID });
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    /*
    kafka4 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true });
    kafka4.createService({
        name: "math",
        actions: {
            add(ctx) {
                this.logger.debug("math.add", { nodeID: this.broker.nodeID });
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    */
    
    await kafka1.start();
    await kafka2.start();
    await kafka3.start();
    // await kafka4.start();
    
    await kafka1.waitForServices(["math"]);

    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let ts = Date.now();
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            let b = count;
            let result = await kafka1.call("math.add", { a: 5, b: b });
            count ++;
            if (!result || result !== (5 + b)) kafka1.logger.error("result", { result });
            kafka1.logger.debug("result", { result });
            return result;
        }));
        let te = Date.now();
        console.log({
            "package completed": {
                "package": i,
                "handler calls": count,
                "time (ms)": te-ts
            }
        });
    }
    let tf = Date.now();
    console.log({
        "handler completed": {
            "handler calls": count,
            "time (ms)": tf-ts
        }
    });
    let result = await kafka1.call("$node.services");
    kafka1.logger.debug("result", { result });
    
    await kafka1.stop();
    await kafka2.stop();
    await kafka3.stop();
    // await kafka4.stop();

    console.log("-------------------");
    wtf.dump();
    // console.log("handles:", process._getActiveHandles());
    // console.log("requests:", process._getActiveRequests());
    console.log("-------------------\n");
    
})();

