package org.functionstream.gateway.handler;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.mutiny.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;

/**
 * @author HALOXIAO
 * @since 2021/7/21
 */
@ApplicationScoped
@Slf4j
public class VerticleDeployHandler {



    public void init(@Observes StartupEvent e, Vertx vertx, Instance<AbstractVerticle> verticles) {
        log.info("deploy verticle");
        for (AbstractVerticle verticle : verticles) {
            vertx.deployVerticle(verticle).await().indefinitely();
        }
    }
}
