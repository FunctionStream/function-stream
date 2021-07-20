package org.functionstream.gateway.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.gateway.common.BusAddressConst;

import java.time.LocalDateTime;

/**
 * @author HALOXIAO
 * @since 2021/7/20
 */
@Slf4j
public class CoreServerVerticle extends AbstractVerticle {


    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(BusAddressConst.HEALTH_CHECK, this::healthCheckServer);
    }

    /**
     * health check
     */
    private void healthCheckServer(Message<String> msg) {
        msg.reply(LocalDateTime.now().toString());
    }

}
