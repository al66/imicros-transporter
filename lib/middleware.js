/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

module.exports ={

    // When event is emitted
    emit(next) {
        return async function emitter(eventName, payload, opts) {            

            // add dummy to force event emitting - also if there is no one listening
            await this.broker.registry.events.add(this.broker.nodeID, "global", { name: eventName, group: "global" });

            // Call default handler
            return next(eventName, payload, opts);
        };
    },
    
    created(broker) {
        this.broker = broker;
    }
  
};