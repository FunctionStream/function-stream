package org.function.stream.gateway.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

/**
 * common rest method, such security
 *
 * @author HALOXIAO
 * @since 2021/6/25
 */
@Slf4j
public class BaseControllerVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        super.start(startPromise);
    }

}
