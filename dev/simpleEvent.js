"use strict";
let Promise = require("bluebird");
const { v4: uuid } = require("uuid");
const wtf = require("wtfnode");

const { ServiceBroker } = require("moleculer");
const Transporter = require("../lib/transporter");

// KAFKA
let kafka1;
let kafka2;
(async function () {

    const n = 300000;
    const p = 100000;
    
    let received = 0;
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter({ brokers: ["192.168.2.124:9092"] }), disableBalancer: true });

    kafka2 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter({ brokers: ["192.168.2.124:9092"] }), disableBalancer: true });
    kafka2.createService({
        name: "events",
        events: {
            "account.*": {
                handler(ctx) {
                    received++;
                    // this.logger.info("Event received, parameters OK!", ctx.params);
                }
            }
        }
    });
    
    await kafka1.start();
    await kafka2.start();
    
    await kafka1.waitForServices(["events"]);

    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async (i) => {
            await kafka1.emit("account.created", { name: "user " + i });
            count ++;
            // await kafka1.emit("user.confirmed", { name: "user " + i });
            // count ++;
        }));
    }
    let tf = Date.now();
    console.log({
        "emmited completed": {
            "emitted events": count,
            "received events": received,
            "time (ms)": tf-ts
        }
    });
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
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

    console.log("-------------------");
    wtf.dump();
    // console.log("handles:", process._getActiveHandles());
    // console.log("requests:", process._getActiveRequests());
    console.log("-------------------\n");
    
})();

