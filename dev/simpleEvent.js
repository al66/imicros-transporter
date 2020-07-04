"use strict";
let Promise = require("bluebird");
const { v4: uuid } = require("uuid");
const wtf = require("wtfnode");

const { ServiceBroker } = require("moleculer");
const Transporter = require("../lib/kafka-nats-2");
const Events = require("../lib/middleware");

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


let anyData = [];
for (let i=0; i<100; i++) anyData.push(uuid());


// KAFKA
let kafka1;
let kafka2;
let kafka3;
(async function () {

    const n = 1000000;
    const p = 20000;

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    function setImmediatePromise() {
        return new Promise((resolve) => {
            setImmediate(() => resolve());
        });
    }    
    
    let received = 0;
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true, middlewares: [Events] });

    kafka2 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(transporterSettings), disableBalancer: true });
    await kafka2.createService({
        name: "events",
        events: {
            "account.created": {
                group: "worker",
                handler(ctx) {
                    received++;
                    // this.logger.info("Event received, parameters OK!", ctx.params);
                }
            }
        }
    });
    kafka3 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter({ brokers: ["192.168.2.124:9092"] }), disableBalancer: true });
    kafka3.createService({
        name: "events",
        events: {
            "account.created": {
                group: "worker",
                handler(ctx) {
                    received++;
                    // this.logger.info("Event received, parameters OK!", ctx.params);
                }
            }
        }
    });
    
    await kafka1.start();
    await kafka2.start();
    // await kafka3.start();
    // await sleep(1000);
    
    // await kafka1.waitForServices(["events"]);

    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let ts = Date.now();
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async (i) => {
            await kafka1.emit("account.created", { 
                name: "user " + i
                // attributes: anyData
            });
            // await setImmediatePromise();
            // await kafka1.emit("account.created", { name: "user " + i });
            count ++;
            // await kafka1.emit("user.passwordResetRequested", { name: "user " + i });
            // count ++;
        }));
        let te = Date.now();
        console.log({
            "package completed": {
                "emitted events": p,
                "received events": received,
                "time (ms)": te-ts
            }
        });
    }
    let tf = Date.now();
    console.log({
        "emmited completed": {
            "emitted events": count,
            "received events": received,
            "time (ms)": tf-ts
        }
    });
    while (received < count) { 
        await sleep(10);
    }
    tf = Date.now();
    console.log({
        "received completed": {
            "handler calls": count,
            "received events": received,
            "time (ms)": tf-ts
        }
    });
    
    await kafka1.stop();
    await kafka2.stop();
    // await kafka3.stop();

    console.log("-------------------");
    wtf.dump();
    // console.log("handles:", process._getActiveHandles());
    // console.log("requests:", process._getActiveRequests());
    console.log("-------------------\n");
    
})();

