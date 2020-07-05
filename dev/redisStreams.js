
const Redis = require("ioredis");

const { v4: uuid } = require("uuid");

process.env.REDIS_HOST = "192.168.2.124";
process.env.REDIS_PORT = 6379;
process.env.REDIS_AUTH = "";
process.env.REDIS_DB = 0;


const redis = new Redis({
    port: process.env.REDIS_PORT || 6379,
    host: process.env.REDIS_HOST || "127.0.0.1",
    password: process.env.REDIS_AUTH || "",
    db: process.env.REDIS_DB || 0,
});

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let redisStopped, redisConnected;

let stream = "my.action." + Date.now();
let group = stream;
let groupExists = false;

function connectRedis () {
    return new Promise((resolve, reject) => {

        redis.on("connect", (() => {
            redisConnected = true;
            console.log("Connected to Redis");
            resolve();
        }));

        redis.on("close", (() => {
            redisConnected = false;
            console.log("Disconnected from Redis");
        }));

        /* istanbul ignore next */
        redis.on("error", ((err) => {
            console.log("Redis redis error", err.message);
            /* istanbul ignore else */
            if (!redisConnected) reject(err);
        }));

    });
}

function disconnectRedis () {
        
    redisStopped = true;
    return new Promise((resolve) => {
            /* istanbul ignore else */
        if (redis && redisConnected) {
            redis.on("close", () => {
                resolve();
            });
            redis.disconnect();
        } else {
            resolve();
        }
    });
}

async function checkGroup () {
    if (!groupExists) { 
        await redis.xgroup("CREATE", stream, group, "0", "MKSTREAM");
        console.log("Group created", { stream, group });
        groupExists = true;
    }
    // get registered groups for the stream
    /*
    try {
        let groups = await redis.xinfo("GROUPS", stream);
        groups.map((g) => { if (g[1] === group) groupExists = true; });
        if (!groupExists) { 
            await redis.xgroup("CREATE", stream, group, "0");
            console.log("Group created", { stream, group });
            groupExists = true;
        }
    } catch (err) {
        console.log("GROUPS create error", { stream, group, error: err });
    }
    */
}

let received = 0;

function listen() {
    let consumer = uuid();
    let streams = [stream];
    let keys = streams.map(() => ">"); 
    let redisRunner;

    async function read () {
        if (redisStopped || !redisConnected) return redisRunner ? clearTimeout(redisRunner) : null ;

        try {
            
            let result = await redis.xreadgroup("GROUP",group, consumer, "COUNT", 100, "STREAMS", streams, keys); 
            
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
                                        console.log("Failed parsing message", { stream: stream, id: a[m][0] });
                                        message[fields[f]] = fields[f+1];
                                    }
                                }   
                            } else {
                                message[fields[f]] = fields[f+1];
                            }
                        }
                        messages.push(message);
                        // simulate some processing time
                        // await sleep(50);
                        let ack = await redis.xack(stream, group, a[m][0]); 
                        received++;
                    }
                    // this.streamIDs[index] = a[a.length-1][0];
                }
                // console.log("Received", { messages });
            } else {
                // console.log("Nothing received");
            }
        } catch (err) {
            console.log("read from redis failed", { err });
        }
    }
    
    // start runner
    redisRunner = setInterval(async () => await read(), 1);  
    console.log("runner started", { stream, group, consumer });
    return redisRunner;
}

let sent = 0;
async function send () {

    let message = JSON.stringify({ value: "this is the message"});
    let maxlen = 1000000;      // currently the best option...

    await redis.xlen(stream);
    await redis.xadd(stream,"MAXLEN","~",maxlen,"*","message", message, "time", Date.now());
    await sleep(5);
    sent ++;

}

async function info () {
    let streams = await redis.xinfo("STREAM", stream);
    console.log(streams);
    let groups = await redis.xinfo("GROUPS", stream);
    console.log(groups);
}

let n = 500000;
let p = 20000;

async function run () {
    
    await connectRedis();
    
    await checkGroup();
    
    await listen();
    await listen();
    await listen();

    let ts = Date.now();
    for (let i = 0; i < n; i+= p) {
        let ts = Date.now();
        let calls = Array.from(Array(p),(x,i) => i);
        await Promise.all(calls.map(async () => {
            // await sleep(5);
            await send();
        }));
        let te = Date.now();
        console.log({
            package: i,
            sent: sent,
            received: received,
            time: te-ts
        });
    }
        
    let te = Date.now();
    console.log({
        sent: sent,
        received: received,
        time: te-ts
    });
    
    while (received < sent) { 
        await sleep(10);
    }

    let tf = Date.now();
    console.log({
        sent: sent,
        received: received,
        time: tf-ts
    });
    
    await info();
    
    await disconnectRedis();
}

run();
