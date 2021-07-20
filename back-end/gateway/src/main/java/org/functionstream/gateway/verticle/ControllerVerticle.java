package org.functionstream.gateway.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

/**
 * @author HALOXIAO
 * @since 2021/7/20
 */
@Slf4j
public class ControllerVerticle extends AbstractVerticle {


    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        router.route("/health/check").handler(this::healthCheck);

    }


    private void healthCheck(RoutingContext context) {
        context.response()
                .setStatusCode(200)
                .end(LocalDateTime.now().toString());
    }

}
