package org.functionstream.gateway.verticle;

import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.mutiny.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.gateway.common.BusAddressConst;
import org.functionstream.gateway.service.ServerService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.LocalDateTime;

/**
 * @author HALOXIAO
 * @since 2021/7/20
 */
@Slf4j
@ApplicationScoped
public class CoreServerVerticle extends AbstractVerticle {

    @Inject
    ServerService serverService;

    @Override
    public void start() {
        vertx.eventBus().consumer(BusAddressConst.HEALTH_CHECK, this::healthCheckServer);
    }

    /**
     * health check
     */
    private void healthCheckServer(Message<String> msg) {
        msg.reply(LocalDateTime.now().toString());
    }

}
