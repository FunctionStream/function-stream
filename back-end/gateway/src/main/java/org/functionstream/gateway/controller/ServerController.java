package org.functionstream.gateway.controller;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.eventbus.Message;
import org.functionstream.gateway.common.BusAddressConst;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * @author HALOXIAO
 * @since 2021/7/21
 */
@Path("/server")
public class ServerController {


    @Inject
    EventBus eventBus;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/health")
    public Uni<String> healthCheck() {
        return eventBus.<String>request(BusAddressConst.HEALTH_CHECK, null)
                .onItem().transform(Message::body);
    }



}
