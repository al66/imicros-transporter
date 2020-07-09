/**
 * @jest-environment node
 *
 * Tests from https://github.com/moleculerjs/moleculer/blob/master/test/transporters/suites/streaming.js
 */

"use strict";
const { ServiceBroker }     = require("moleculer");
const { KafkaNats }         = require("../index");
const { EventsMiddleware }  = require("../index");

const fs 								   = require("fs");
const crypto 							= require("crypto");

const iv = Buffer.from(crypto.randomBytes(16));
const password = Buffer.from(crypto.randomBytes(32));

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

const AESService = {
    name: "aes",
    actions: {
        encrypt(ctx) {
            const encrypter = crypto.createCipheriv("aes-256-ctr", password, iv);
            return ctx.params.pipe(encrypter);
        },

        decrypt(ctx) {
            const decrypter = crypto.createDecipheriv("aes-256-ctr", password, iv);
            return ctx.params.pipe(decrypter);
        }
    }
};

const filename = __dirname + "/assets/imicros.png";
const filename2 = __dirname + "/assets/received.png";

function getSHA(filename) {
    return new Promise((resolve, reject) => {
        let hash = crypto.createHash("sha1");
        let stream = fs.createReadStream(filename);
        stream.on("error", err => reject(err));
        stream.on("data", chunk => hash.update(chunk));
        stream.on("end", () => resolve(hash.digest("hex")));
    });
}

function protectReject(err) {
    if (err && err.stack) {
        console.error(err);
        console.error(err.stack);
    }
    expect(err).toBe(true);
}

describe("Test streaming", () => {

    const [master, slaveA, slaveB] = ["master", "slaveA", "slaveB"].map(nodeID => {
        return new ServiceBroker({
            namespace: "streaming",
            nodeID: nodeID,
            transporter: new KafkaNats(transporterSettings),
            // transporter: "nats://192.168.2.124:4222",
            // disableBalancer: true,                   // does not work with streaming!!!! - refer to transit.js !
            middlewares: [EventsMiddleware],
            logger: false 
        });        
    });

    // Load services
    [slaveA, slaveB].forEach(broker => broker.createService(AESService));
    // [slaveA].forEach(broker => broker.createService(AESService));

    let originalHash;
    
    // Start & Stop
    beforeAll(() => {
        return Promise.all([master.start(), slaveA.start(), slaveB.start()])
            .then(() => getSHA(filename))
            .then(hash => originalHash = hash);
    });
    afterAll(() => Promise.all([master.stop(), slaveA.stop(), slaveB.stop()]));
    

    it("should encode & decode the data and send as streams", () => {
        return master.waitForServices("aes")
            .delay(500)
            .then(() => Promise.all(Array.from(Array(1),(x,i) => i).map( () => {
                const s1 = fs.createReadStream(filename);
                return master.call("aes.encrypt", s1)
                  .then(s2 => master.call("aes.decrypt", s2))
                  .then(s3 => {
                      return new Promise(resolve => {
                          const s4 = fs.createWriteStream(filename2);
                          s4.on("close", () => getSHA(filename2).then(hash => resolve(hash)));
                          s3.pipe(s4);
                      });
                  })
                  .catch(protectReject)
                  .then(hash => {
                      expect(hash).toBe(originalHash);
                      fs.unlinkSync(filename2);
                  });
            })));
    }, 30000);
    
});


