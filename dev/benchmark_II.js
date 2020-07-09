"use strict";

const { v4: uuid } = require("uuid");


const n = 100000;
const p = 10000;

let result = [];
let running = 0;

// Local
let local;
(async function () {
    running++;
    
    const { ServiceBroker } = require("moleculer");
    local = new ServiceBroker({ nodeID: "node-1" });

    local.createService({
        name: "math",
        actions: {
            add(ctx) {
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    await local.start();

    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            await local.call("math.add", { a: 5, b: 3 });
            count ++;
        }));
    }
    let tf = Date.now();
    result.push({
        "test": "local",
        "completed": {
            "calls": count,
            "time (ms)": tf-ts
        }
    });

    await local.stop();
    
    running--;
})();

// TCP
let transporterTCP = "TCP";1;
let tcp1;
let tcp2;
(async function () {
    running++;

    const { ServiceBroker } = require("moleculer");
    tcp1 = new ServiceBroker({ nodeID: "node-1", transporter: transporterTCP });
    tcp2 = new ServiceBroker({ nodeID: "node-2", transporter: transporterTCP });

    tcp2.createService({
        name: "math",
        actions: {
            add(ctx) {
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    await tcp1.start();
    await tcp2.start();

    await tcp1.waitForServices(["math"]);    
    
    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            await tcp1.call("math.add", { a: 5, b: 3 });
            count ++;
        }));
    }
    let tf = Date.now();
    result.push({
        "test": "TCP",
        "completed": {
            "calls": count,
            "time (ms)": tf-ts
        }
    });

    await tcp1.stop();
    await tcp2.stop();
    
    running--;
})();

// NATS
let transporterNATS = "nats://192.168.2.124:4222";
let nats1;
let nats2;
(async function () {
    running++;

    const { ServiceBroker } = require("moleculer");
    nats1 = new ServiceBroker({ nodeID: "node-1", transporter: transporterNATS });
    nats2 = new ServiceBroker({ nodeID: "node-2", transporter: transporterNATS });

    nats2.createService({
        name: "math",
        actions: {
            add(ctx) {
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    await nats1.start();
    await nats2.start();

    await nats1.waitForServices(["math"]);    
    
    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            await nats1.call("math.add", { a: 5, b: 3 });
            count ++;
        }));
    }
    let tf = Date.now();
    result.push({
        "test": "NATS",
        "completed": {
            "calls": count,
            "time (ms)": tf-ts
        }
    });

    await nats1.stop();
    await nats2.stop();
    
    running--;
})();

// KAFKA
let kafka1;
let kafka2;
(async function () {
    running++;

    const opts = {
        kafka: {
            brokers: ["192.168.2.124:9092"]
        },
        nats: {
            url: "nats://192.168.2.124:4222"
        }
    };
    const { ServiceBroker } = require("moleculer");
    const Transporter = require("../lib/kafka-nats");
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(opts) });
    kafka2 = new ServiceBroker({ nodeID: uuid(), transporter: new Transporter(opts) });

    await kafka2.createService({
        name: "math",
        actions: {
            add(ctx) {
                return ctx.params.a + ctx.params.b;
            }
        }
    });
    await kafka1.start();
    await kafka2.start();
    await kafka1.waitForServices(["math"]);
    
    let ts = Date.now();
    let count = 0;
    for (let i = 0; i < n; i+= p) {
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            await kafka1.call("math.add", { a: 5, b: 3 });
            count ++;
        }));
    }
    let tf = Date.now();
    result.push({
        "test": "kafka-nats",
        "completed": {
            "calls": count,
            "time (ms)": tf-ts
        }
    });

    await kafka1.stop();
    await kafka2.stop();
    
    running--;
})();

(async function () {
    let timer;
    function ready() {
        if (running <= 0) {
            console.log(result);
            clearInterval(timer);
        }
    }
    timer = setInterval(ready, 50);
})();