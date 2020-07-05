"use strict";

let Promise = require("bluebird");
let Benchmarkify = require("benchmarkify");
const { v4: uuid } = require("uuid");

let benchmark = new Benchmarkify("Microservices benchmark").printHeader();

const bench = benchmark.createSuite("Call remote actions");

// Local
let local;
(async function () {

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

    bench.add("local", done => {
        local.call("math.add", { a: 5, b: 3 }).then(done);
    });

})();

// TCP
let transporterTCP = "TCP";1;
let tcp1;
let tcp2;
(async function () {

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

    bench.add("TCP", done => {
        tcp1.call("math.add", { a: 5, b: 3 }).then(done);
    });

})();

// NATS
let transporterNATS = "nats://192.168.2.124:4222";
let nats1;
let nats2;
(async function () {

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

    bench.add("NATS", done => {
        nats1.call("math.add", { a: 5, b: 3 }).then(done);
    });

})();

// KAFKA-NATS
let kafka1;
let kafka2;
(async function () {

    const opts = {
        kafka: {
            brokers: ["192.168.2.124:9092"]
        },
        nats: {
            url: "nats://192.168.2.124:4222"
        }
    };
    const { ServiceBroker } = require("moleculer");
    const { KafkaNats } = require("../index");
    kafka1 = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(opts), disableBalancer: true });
    kafka2 = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(opts), disableBalancer: true });

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
    
    bench.add("kafka-nats", done => {
        kafka1.call("math.add", { a: 5, b: 3 }).then(done);
    });

})();

Promise.delay(10000).then(() => {

    return benchmark.run([bench]).then(() => {

        local.stop();
        
        tcp1.stop();
        tcp2.stop();

        nats1.stop();
        nats2.stop();

        kafka1.stop();
        kafka2.stop();

    });

});