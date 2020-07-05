# imicros-transporter
[![Build Status](https://travis-ci.org/al66/imicros-transporter.svg?branch=master)](https://travis-ci.org/al66/imicros-transporter)
[![Coverage Status](https://coveralls.io/repos/github/al66/imicros-transporter/badge.svg?branch=master)](https://coveralls.io/github/al66/imicros-transporter?branch=master)
[![Development Status](https://img.shields.io/badge/status-experimental-orange)](https://img.shields.io/badge/status-experimental-orange)

Two way transporter for [Moleculer](https://github.com/moleculerjs/moleculer) services
- actions/responses and registry messages non-persistent via [NATS](https://nats.io/) 
- events persistent via [kafka](https://kafka.apache.org/) 

## Installation
```
$ npm install imicros-transporter --save
```
Requires installation of the additional modules [nats](https://www.npmjs.com/package/nats) and [kafkajs](https://www.npmjs.com/package/kafkajs)

# Usage

```
const { KafkaNats } = require("imicros-transporter");

const transporterSettings = {
    kafka: {
        brokers: ["192.168.2.124:9092"]
        /* optional: additional settings
        ssl: null,                           // refer to kafkajs documentation
        sasl: null,                          // refer to kafkajs documentation
        retry: {
             initialRetryTime: 100,          // default 100
             retries: 8                      // default 8
        }
        */
    },
    nats: {
        url: "nats://192.168.2.124:4222"
        /* optional: additional settings
        user: "admin",
        pass: "1234"
        */
    }
};
kafka = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(transporterSettings), disableBalancer: true });

```

## Force emittung events always

If no running service has subscribed an event, it will not be emitted in the current moleculer version.

To force to emit always - also if no one is listening, we must add a small middleware:
```
const { KafkaNats } = require("imicros-transporter");
const { EventsMiddleware } = require("imicros-transporter");

kafka = new ServiceBroker({ nodeID: uuid(), transporter: new KafkaNats(transporterSettings), disableBalancer: true, middlewares: [EventsMiddleware] });

```

