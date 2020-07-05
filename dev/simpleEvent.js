"use strict";
let Promise = require("bluebird");
const { v4: uuid } = require("uuid");
const wtf = require("wtfnode");

const { ServiceBroker } = require("moleculer");
const { KafkaNats } = require("../index");
const { EventsMiddelware } = require("../index");

process.env.REDIS_HOST = "192.168.2.124";
process.env.REDIS_PORT = 6379;
process.env.REDIS_AUTH = "";
process.env.REDIS_DB = 0;

const transporterSettings = {
    kafka: {
        brokers: ["192.168.2.124:9092"],
        retry: {
            initialRetryTime: 100,          // default 100
            retries: 8                      // default 8
        }
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
(async function () {

    const n = 20000;    
    const p = 10000;

    const c = 1;
    
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    let received = 0;
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(transporterSettings), disableBalancer: true, middlewares: [EventsMiddelware] });

    let listener = [];
    let calls = [];
    for ( let i = 0; i < c; i ++) {
        let kafka = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(transporterSettings), disableBalancer: true });
        await kafka.createService({
            name: "events",
            events: {
                "account.created": {
                    group: "worker",
                    handler(ctx) {
                        received++;
                        calls[this.broker.nodeID] ? calls[this.broker.nodeID]++ : calls[this.broker.nodeID] = 1;
                        // this.logger.info("Event received, parameters OK!", ctx.params);
                    }
                }
            }
        });
        await kafka.start();
        await kafka.waitForServices(["events"]);
        listener.push(kafka);
    }
    
    await kafka1.start();
    await sleep(2000);
    // await kafka1.waitForServices(["events"]); // doesn't work, doesn't wait for subscriptions.. :-(

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
    let timer;
    async function ready () {
        if (received < count) return;
        clearInterval(timer);
        
        tf = Date.now();
        console.log({
            "received completed": {
                "handler calls": count,
                "received events": received,
                "by nodes": calls,
                "time (ms)": tf-ts
            }
        });

        await kafka1.stop();
        await Promise.all(listener.map(async (kafka) => await kafka.stop()));

        console.log("-------------------");
        wtf.dump();
        // console.log("handles:", process._getActiveHandles());
        // console.log("requests:", process._getActiveRequests());
        console.log("-------------------\n");
    
    }
    timer = setInterval(ready, 10);
    
})();

