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

const mathService = {
    name: "math",
    actions: {
        add(ctx) {
            return {
                node: this.broker.nodeID,
                result: Number(ctx.params.a) + Number(ctx.params.b)
            };
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

describe("Test normal calling", () => {

    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "test",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true,                    // does not work with streaming!!!! - refer to transit.js !
            middlewares: [EventsMiddleware],
            logger: false 
        });        
    });

    // Load services
    [slaveA, slaveB, slaveC].forEach(broker => broker.createService(mathService));

    // Start & Stop
    beforeAll(() => Promise.all([master.start(), slaveA.start(), slaveB.start()]));
    afterAll(() => Promise.all([master.stop(), slaveA.stop(), slaveB.stop()]));
    
    it("should call actions with balancing between 2 nodes", () => {
        return master.waitForServices("math")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map( () => master.call("math.add", { a: 50, b: 13 }))))
                .catch(protectReject)
                .then(res => {
                    //console.log(res);
                    expect(res).toHaveLength(6);
                    expect(res.filter(o => o.result == 63)).toHaveLength(6);
                    expect(res.filter(o => o.node == "slaveA")).toHaveLength(3);
                    expect(res.filter(o => o.node == "slaveB")).toHaveLength(3);
                });
    });

    it("start slaveC", () => {
        return slaveC.start().delay(500);
    });

    it("should call actions with balancing between 3 nodes", () => {
        return master.waitForServices("math")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("math.add", { a: 20, b: 30 }))))
                .catch(protectReject)
                .then(res => {
                 //console.log(res);
                    expect(res).toHaveLength(6);
                    expect(res.filter(o => o.result == 50)).toHaveLength(6);
                    expect(res.filter(o => o.node == "slaveA")).toHaveLength(2);
                    expect(res.filter(o => o.node == "slaveB")).toHaveLength(2);
                    expect(res.filter(o => o.node == "slaveC")).toHaveLength(2);
                });
    });

    it("stop slaveC", () => {
        return slaveC.stop().delay(500);
    });

    it("should call actions without slaveC node", () => {
        return master.waitForServices("math")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("math.add", { a: 20, b: 30 }))))
                .catch(protectReject)
                .then(res => {
                 //console.log(res);
                    expect(res).toHaveLength(6);
                    expect(res.filter(o => o.result == 50)).toHaveLength(6);
                    expect(res.filter(o => o.node == "slaveA")).toHaveLength(3);
                    expect(res.filter(o => o.node == "slaveB")).toHaveLength(3);
                    expect(res.filter(o => o.node == "slaveC")).toHaveLength(0);
                });
    });


    it("should direct call action on the specified node", () => {
        return master.waitForServices("math")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("math.add", { a: 20, b: 30 }, { nodeID: "slaveB" }))))
                .catch(protectReject)
                .then(res => {
                 //console.log(res);
                    expect(res).toHaveLength(6);
                    expect(res.filter(o => o.result == 50)).toHaveLength(6);
                    expect(res.filter(o => o.node == "slaveA")).toHaveLength(0);
                    expect(res.filter(o => o.node == "slaveB")).toHaveLength(6);
                });
    });    
    
});

describe("Test normal calling with versioned services", () => {

    // Creater brokers
    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "version-call",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true,                        // does not work with streaming!!!! - refer to transit.js !
            middlewares: [EventsMiddleware],
            logger: false 
        });        
    });     
     
    // Load services
    slaveA.createService(Object.assign({}, mathService, { version: 2 }));
    slaveB.createService(Object.assign({}, mathService, { version: "beta" }));
    slaveC.createService(mathService);

    // Start & Stop
    beforeAll(() => Promise.all([master.start(), slaveA.start(), slaveB.start(), slaveC.start()]));
    afterAll(() => Promise.all([master.stop(), slaveA.stop(), slaveB.stop(), slaveC.stop()]));

    it("should call numeric versioned service", () => {
        return master.waitForServices({ name: "math", version: 2 })
            .delay(500)
            .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("v2.math.add", { a: 50, b: 13 }))))
            .catch(protectReject)
            .then(res => {
             //console.log(res);
                expect(res).toHaveLength(6);
                expect(res.filter(o => o.result == 63)).toHaveLength(6);
                expect(res.filter(o => o.node == "slaveA")).toHaveLength(6);
            });
    });

    it("should call string versioned service", () => {
        return master.waitForServices({ name: "math", version: "beta" })
            .delay(500)
            .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("beta.math.add", { a: 50, b: 13 }))))
            .catch(protectReject)
            .then(res => {
             //console.log(res);
                expect(res).toHaveLength(6);
                expect(res.filter(o => o.result == 63)).toHaveLength(6);
                expect(res.filter(o => o.node == "slaveB")).toHaveLength(6);
            });
    });

    it("should call unversioned service", () => {
        return master.waitForServices({ name: "math" })
            .delay(500)
            .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.call("math.add", { a: 50, b: 13 }))))
            .catch(protectReject)
            .then(res => {
             //console.log(res);
                expect(res).toHaveLength(6);
                expect(res.filter(o => o.result == 63)).toHaveLength(6);
                expect(res.filter(o => o.node == "slaveC")).toHaveLength(6);
            });
    });


});

