package com.github.hongkaiwen.vertx_play;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;

public class MainVerticle extends AbstractVerticle {

  private JDBCClient client;


  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    // creates the jdbc client.
    client = JDBCClient.createNonShared(vertx, config());


    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);

    router.get("/insert").handler(request -> {
      client.query("INSERT INTO metering_record(quantity) VALUE (1000)", event -> {
        request.
          response()
          .putHeader("content-type", "application/text")
          .end(String.format("success: %b", event.succeeded()));
        if(event.cause() != null){
          event.cause().printStackTrace();
        }
      });
    });

    server.requestHandler(router);
    server.listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

}
