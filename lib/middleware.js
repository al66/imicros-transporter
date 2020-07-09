/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

module.exports ={

    // When event is emitted
    emit(next) {
        return async function emitter(eventName, payload, opts) {            

            // add dummy to force event emitting - also if there is no one listening...
            // ... and delete all others to avoid multiple sending in case of enabled balancer
            this.broker.registry.events.add(this.broker.nodeID, "global", { name: eventName, group: "global" });
            this.broker.registry.events.events = this.broker.registry.events.events.filter(event => event.group === "global");
            this.logger.debug(this.broker.registry.events.events);

            // Call default handler
            return next(eventName, payload, opts);
        };
    },
    
    created(broker) {
        this.broker = broker;
    }
  
};