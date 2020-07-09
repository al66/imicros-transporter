/**
 * @jest-environment node
 *
 * Tests from https://github.com/moleculerjs/moleculer/blob/master/test/transporters/suites/call.js
 */

"use strict";
const { ServiceBroker } = require("moleculer");
const { KafkaNats } = require("../index");
const { EventsMiddleware } = require("../index");

const transporterSettings = {
    kafka: {
        brokers: process.env.KAFKA_BROKER ? [process.env.KAFKA_BROKER] : ["127.0.0.1:9092"],
        retry: {
            initialRetryTime: 100,          // default 100
            retries: 8                      // default 8
        }
    },
    nats: {
        url: process.env.NATS_URL || "127.0.0.1:4222"
    }
};

let calls = [];
const eventSubscriber = {
    name: "events",
    events: {
        "account.created": {
            group: "worker",
            // strategy: "RoundRobin",
            handler(ctx) {
                if (!calls[ctx.params.test]) calls[ctx.params.test] = [];
                calls[ctx.params.test].push({ node: this.broker.nodeID, result: Number(ctx.params.a) + Number(ctx.params.b) });
                // calls[this.broker.nodeID] ? calls[this.broker.nodeID]++ : calls[this.broker.nodeID] = 1;
                // this.logger.info("Event received, parameters OK!", ctx.params);
            }
        }
    }
};

function protectReject(err) {
    if (err && err.stack) {
        console.error(err);
        console.error(err.stack);
    }
    expect(err).toBe(true);
}

describe("Test events", () => {

    const n = 500;
    
    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "events",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true,                           // does not work with streaming!!!! - refer to transit.js !
            middlewares: [EventsMiddleware],
            // logLevel: "debug" //"debug"
            logger: false 
        });        
    });

    // Load services
    [slaveA, slaveB, slaveC].forEach(broker => broker.createService(eventSubscriber));

    // Start & Stop
    beforeAll(() => Promise.all([master.start(), slaveA.start(), slaveB.start()]));
    afterAll(() => Promise.all([master.stop(), slaveA.stop(), slaveB.stop()]));
    
    // describe("Test calls", () => {
    it("should process events  with balancing between 2 nodes", () => {
        calls = [];
        return master.waitForServices("events")
                .delay(5000)
                .then(() => Promise.all(Array.from(Array(n),(x,i) => i).map(() => master.emit("account.created", { test: "A", a: 50, b: 13 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls["A"]).toHaveLength(n);
                    expect(calls["A"].filter(o => o.result == 63)).toHaveLength(n);
                    //expect(calls.filter(o => o.node == "slaveA").length).toBeGreaterThanOrEqual(1);
                    //expect(calls.filter(o => o.node == "slaveB").length).toBeGreaterThanOrEqual(1);
                });
    }, 30000);

    it("start slaveC", () => {
        return slaveC.start().delay(5000);
    }, 30000);

    it("should process events  with balancing between 3 nodes", () => {
        calls = [];
        return master.waitForServices("events")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(n),(x,i) => i).map(() => master.emit("account.created", { test: "B", a: 20, b: 30 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls["B"]).toHaveLength(n);
                    expect(calls["B"].filter(o => o.result == 50)).toHaveLength(n);
                    expect(calls["B"].filter(o => o.node == "slaveA").length).toBeGreaterThanOrEqual(1);
                    expect(calls["B"].filter(o => o.node == "slaveB").length).toBeGreaterThanOrEqual(1);
                    expect(calls["B"].filter(o => o.node == "slaveC").length).toBeGreaterThanOrEqual(1);
                });
    });

    it("stop slaveC", () => {
        return slaveC.stop().delay(5000);
    }, 30000);

    it("should process events without slaveC node", () => {
        calls = [];
        return master.waitForServices("events")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(n),(x,i) => i).map(() => master.emit("account.created", { test: "C", a: 20, b: 30 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls["C"]).toHaveLength(n);
                    expect(calls["C"].filter(o => o.result == 50)).toHaveLength(n);
                    expect(calls["C"].filter(o => o.node == "slaveA").length).toBeGreaterThanOrEqual(1);
                    expect(calls["C"].filter(o => o.node == "slaveB").length).toBeGreaterThanOrEqual(1);
                    expect(calls["C"].filter(o => o.node == "slaveC")).toHaveLength(0);
                });
    });


});


describe("Test persistent events", () => {

    const n = 500;
    
    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "events",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true,                       // does not work with streaming!!!! - refer to transit.js !
            middlewares: [EventsMiddleware],
            // logLevel: "info" //"debug"
            logger: false 
        });        
    });

    // Load services
    [slaveA, slaveB, slaveC].forEach(broker => broker.createService(eventSubscriber));

    // Start & Stop
    // beforeAll(() => Promise.all([master.start()]));
    afterAll(() => Promise.all([slaveA.stop(), slaveB.stop(), slaveC.stop()]));
    

    it("start master and emit events", () => {
        calls = [];
        return master.start()
                .delay(5000)
                .then(() => Promise.all(Array.from(Array(n),(x,i) => i).map(() => master.emit("account.created", { test: "D", a: 50, b: 13 }))))
                .delay(500);
                //.catch(protectReject);
    }, 30000);
    
    //  Doesn't work yet - event is ignored, when sender is down...
    it("stop master", () => {
        return master.stop().delay(500);
    });

    it("start slaves", () => {
        return Promise.all([slaveA.start(), slaveB.start(), slaveC.start()]).delay(5000);
    }, 30000);

    
    it("should process events  with balancing between 3 nodes", () => {
        return slaveC.waitForServices("events")
                .delay(500)
                // .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls["D"]).toHaveLength(n);
                    expect(calls["D"].filter(o => o.result == 63)).toHaveLength(n);
                    expect(calls["D"].filter(o => o.node == "slaveA").length).toBeGreaterThanOrEqual(1);
                    expect(calls["D"].filter(o => o.node == "slaveB").length).toBeGreaterThanOrEqual(1);
                    expect(calls["D"].filter(o => o.node == "slaveC").length).toBeGreaterThanOrEqual(1);
                });
    }, 30000);


});

