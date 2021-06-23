package org.function.stream.gateway.controller;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;


public class GateWayMainVerticle extends AbstractVerticle {

  private final Router router = Router.router(vertx);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.createHttpServer().listen(8080, http -> {

    });
  }



}
