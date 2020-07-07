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

let calls = [];
const eventSubscriber = {
    name: "events",
    events: {
        "account.created": {
            group: "worker",
            handler(ctx) {
                calls.push({ node: this.broker.nodeID, result: Number(ctx.params.a) + Number(ctx.params.b) });
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

describe("Test normal calling", () => {

    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "test",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true, 
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
            // disableBalancer: true, 
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

describe("Test events", () => {

    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "events",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true, 
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
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.emit("account.created", { a: 50, b: 13 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls).toHaveLength(6);
                    expect(calls.filter(o => o.result == 63)).toHaveLength(6);
                    expect(calls.filter(o => o.node == "slaveA")).toHaveLength(3);
                    expect(calls.filter(o => o.node == "slaveB")).toHaveLength(3);
                });
    });

    it("start slaveC", () => {
        return slaveC.start().delay(500);
    });

    it("should process events  with balancing between 3 nodes", () => {
        calls = [];
        return master.waitForServices("events")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.emit("account.created", { a: 20, b: 30 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls).toHaveLength(6);
                    expect(calls.filter(o => o.result == 50)).toHaveLength(6);
                    expect(calls.filter(o => o.node == "slaveA")).toHaveLength(2);
                    expect(calls.filter(o => o.node == "slaveB")).toHaveLength(2);
                    expect(calls.filter(o => o.node == "slaveC")).toHaveLength(2);
                });
    });

    it("stop slaveC", () => {
        return slaveC.stop().delay(500);
    });

    it("should process events without slaveC node", () => {
        calls = [];
        return master.waitForServices("events")
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.emit("account.created", { a: 20, b: 30 }))))
                .delay(500)
                .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls).toHaveLength(6);
                    expect(calls.filter(o => o.result == 50)).toHaveLength(6);
                    expect(calls.filter(o => o.node == "slaveA")).toHaveLength(3);
                    expect(calls.filter(o => o.node == "slaveB")).toHaveLength(3);
                    expect(calls.filter(o => o.node == "slaveC")).toHaveLength(0);
                });
    });


});

describe("Test persistent events", () => {

    const [master, slaveA, slaveB, slaveC] = ["master", "slaveA", "slaveB", "slaveC"].map(nodeID => {
        return new ServiceBroker({
            namespace: "persistent-events",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            disableBalancer: true, 
            middlewares: [EventsMiddleware],
            // logLevel: "debug" //"debug"
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
                .delay(500)
                .then(() => Promise.all(Array.from(Array(6),(x,i) => i).map(() => master.emit("account.created", { a: 50, b: 13 }))))
                .delay(500);
                //.catch(protectReject);
    });
    
    //  Doesn't work yet - event is ignored, when sender is down...
    it("stop master", () => {
        return master.stop().delay(500);
    });

    it("start slaves", async () => {
        return await Promise.all([slaveA.start(), slaveB.start(), slaveC.start()]).delay(1000);
    }, 30000);

    
    it("should process events  with balancing between 3 nodes", () => {
        return slaveC.waitForServices("events")
                .delay(500)
                // .catch(protectReject)
                .then(() => {
                    // console.log(calls);
                    expect(calls).toHaveLength(6);
                    expect(calls.filter(o => o.result == 63)).toHaveLength(6);
                    expect(calls.filter(o => o.node == "slaveA").length).toBeGreaterThanOrEqual(1);
                    expect(calls.filter(o => o.node == "slaveB").length).toBeGreaterThanOrEqual(1);
                    expect(calls.filter(o => o.node == "slaveC").length).toBeGreaterThanOrEqual(1);
                });
    }, 30000);


});
