/**
 * @license MIT, imicros.de (c) 2020 Andreas Leinen
 */
"use strict";

module.exports ={

    // When event is emitted
    emit(next) {
        /*
        return async function emitter(eventName, payload, opts) {            

            // add dummy to force event emitting - also if there is no one listening...
            // ... and delete all others to avoid multiple sending in case of enabled balancer
            this.broker.registry.events.add(this.broker.nodeID, "global", { name: eventName, group: "global" });
            this.broker.registry.events.events = this.broker.registry.events.events.filter(event => event.group === "global");
            this.logger.debug(this.broker.registry.events.events);

            // Call default handler
            return next(eventName, payload, opts);
        };
        */

        return async function emitter(eventName, payload, opts) {            
            if (Array.isArray(opts) || (typeof opts === "string" || opts instanceof String ))
                opts = { groups: opts };
            else if (opts == null)
                opts = {};

            if (opts.groups && !Array.isArray(opts.groups))
                opts.groups = [opts.groups];

            const ctx = this.ContextFactory.create(this, null, payload, opts);
            ctx.eventName = eventName;
            ctx.eventType = "emit";
            ctx.eventGroups = opts.groups;

            this.logger.debug(`Emit '${eventName}' event`+ (opts.groups ? ` to '${opts.groups.join(", ")}' group(s)` : "") + ".");

              // Call local/internal subscribers
            if (/^\$/.test(eventName))
                this.localBus.emit(eventName, payload);    

            if (this.transit) {

                // Disabled balancer case
                let groups = opts.groups;

                if (!groups || groups.length == 0) {
                    // Apply to all groups
                    groups = this.getEventGroups(eventName);
                }

                ctx.eventGroups = groups;
                return this.transit.sendEvent(ctx);
            }
            
        };
            
            
    },
    
    created(broker) {
        this.broker = broker;
    }
  
};