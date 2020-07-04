/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

const Redis = require("ioredis");

const PACKET_EVENT 			= "EVENT";
const PACKET_REQUEST 		= "REQ";
const PACKET_RESPONSE		= "RES";
const PACKET_DISCOVER 		= "DISCOVER";
const PACKET_INFO 			= "INFO";
const PACKET_DISCONNECT 	= "DISCONNECT";
const PACKET_HEARTBEAT 		= "HEARTBEAT";
const PACKET_PING 			= "PING";
const PACKET_PONG 			= "PONG";

rocess.env.REDIS_HOST = "192.168.2.124";
process.env.REDIS_PORT = 6379;
process.env.REDIS_AUTH = "";
process.env.REDIS_DB = 0;

class Streams {
    
    constructor(opts) {

        this.redisOptions = {
            port: process.env.REDIS_PORT || 6379,
            host: process.env.REDIS_HOST || "127.0.0.1",
            password: process.env.REDIS_AUTH || "",
            db: process.env.REDIS_DB || 0,
        }
        
    }
    
    async connect() { 
        
        await this.connectRedis();
        this.logger.info("connected to redis", { redis: this.redisOptions});
        
    }
    
    async disconnect() { 

        await this.disconnectRedis();
        
    }
    
    async subscribeToRegistryEvents() {
 
        let t = this;
        async function read () {
            if (t.redisStopped) return;

            t.logger.info("get next message from stream");
            let streams = [PACKET_DISCOVER, PACKET_INFO, PACKET_DISCONNECT, PACKET_HEARTBEAT, PACKET_PING, PACKET_PONG];
            let streamIDs = [0,0,0,0,0,0];
            // let streamIDs = ['$','$','$','$','$','$'];
            let result = await t.redis.xread("BLOCK",0,"STREAMS",streams, streamIDs); 

            if (Array.isArray(result)) {
                let messages = [];
                // stream
                for (let s = 0; s<result.length; s++) {
                    let stream = result[s][0];
                    // array of messages
                    let a = result[s][1];
                    for (let m = 0; m<a.length; m++) {
                        let message = {
                            stream: stream,
                            id: a[m][0]
                        };
                        let fields = a[m][1];
                        for (let f = 0; f<fields.length; f+=2 ) {
                            if ( fields[f] === "message" ) {
                                try {
                                    let value = fields[f+1];
                                    message[fields[f]] = value;

                                } catch (e) {
                                    /* could not happen if created with this service */ 
                                    /* istanbul ignore next */ 
                                    {
                                        this.logger.error("Failed parsing message", { stream: stream, id: a[m][0] });
                                        message[fields[f]] = fields[f+1];
                                    }
                                }   
                            } else {
                                message[fields[f]] = fields[f+1];
                            }
                        }
                        messages.push(message);
                    }
                } 
                await Promise.all(messages.map(async (message) => {
                    t.logger.info("received", { 
                        cmd: message.stream,
                        msg: message.message,
                        message
                    });
                    if (!message.stream || !message.message) return;

                    let packet = t.deserialize(message.stream, message.message.toString());
                    let payload = packet.payload;
                    t.logger.info("received", { 
                        cmd: message.stream,
                        packet,
                        sender: payload.sender
                    });
                    // ignore own messages, ignore messages for other nodes
                    if (payload.sender !== t.nodeID || ( packet.target && packet.target !== t.nodeID) )  await t.receive(message.stream, message.message);
                }));
            
                // direct loop
                // if (msg) read();
                // loop
            }
            setTimeout(() => read(), 1000);  
        }
        read();
    }

 
    run () {

        async function 
        // listen to registration events on redis streams
        await this.subscribeToRegistryEvents();
        
    }

    async publishRegistryEvent({ packet }) {
        if (!this.redisConnected) return this.logger.error("Redis not connected - publish registry event failed", { packet });

        // await this.logger.info("publish registry event", { packet });
        
        const data = this.serialize(packet).toString();

        // await this.logger.info("publish registry event - message serializd", { packet });
        let id;
        try {
            this.logger.info("publish registry event - call xadd", { packet, data });
            // id = await this.redis.xadd("registry","MAXLEN","~",10000,"*","message", msg, "time", Date.now());
            // id = await this.redis.xadd("registry","*","message", msg, "time", Date.now());
            /*
            id = await this.redis.sendCommand(
                new Redis.Command("XADD", [packet.type, "*","message", data])
              );
            */
            id = await this.redis.xadd(packet.type,"MAXLEN","~",10000,"*","message", data).timeout(5000);
            await this.logger.info("redis id", { packet, id });
        } catch (err) {
            this.logger.error("Failed to send registry event", { packet, error: err });
            throw err;
        }
        
        this.logger.info("published registry event", {data, packet, id});
        
        return;
    }
    
    
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    connectRedis () {
        return new Promise((resolve, reject) => {

            this.redis = new Redis(this.redisOptions);

            this.redis.on("connect", (() => {
                this.redisConnected = true;
                this.logger.info("Connected to Redis");
                resolve();
            }).bind(this));

            this.redis.on("close", (() => {
                this.redisConnected = false;
                this.logger.info("Disconnected from Redis");
            }).bind(this));

            /* istanbul ignore next */
            this.redis.on("error", ((err) => {
                this.logger.error("Redis redis error", err.message);
                this.logger.debug(err);
                /* istanbul ignore else */
                if (!this.redisConnected) reject(err);
            }).bind(this));
        });
    }        

    async disconnectRedis () {
        
        this.redisStopped = true;
        return new Promise((resolve) => {
            /* istanbul ignore else */
            if (this.redis && this.redisConnected) {
                this.redis.on("close", () => {
                    resolve();
                });
                this.redis.disconnect();
            } else {
                resolve();
            }
        });
    }

}

const test = new Streams({});
test.run();
