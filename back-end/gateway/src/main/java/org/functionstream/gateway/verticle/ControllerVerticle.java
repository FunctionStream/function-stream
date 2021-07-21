package org.functionstream.gateway.verticle;

import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.gateway.common.BusAddressConst;

import java.time.LocalDateTime;

/**
 * @author HALOXIAO
 * @since 2021/7/20
 */
@Slf4j
public class ControllerVerticle extends AbstractVerticle {


    @Override
    public void start(Promise<Void> promise) throws Exception {
        Router router = Router.router(vertx);
        router.route("/health/check").handler(this::healthCheck);
        //TODO 这些配置可以用Guice进行注入
        vertx.createHttpServer().requestHandler(router).listen(8080, res -> {
            if (res.succeeded()) {
                promise.complete();
            } else {
                promise.fail(res.cause());
            }
        });

    }

    //TODO 可以考虑使用AspectJ对所有返回值统一做一个额外处理（如果确实比较复杂的话）
    private void healthCheck(RoutingContext context) {
        vertx.eventBus().request(BusAddressConst.HEALTH_CHECK, null)
                .onSuccess(msg -> {
                    context.response()
                            .setStatusCode(200)
                            .end(msg.body().toString());
                });

    }


}
