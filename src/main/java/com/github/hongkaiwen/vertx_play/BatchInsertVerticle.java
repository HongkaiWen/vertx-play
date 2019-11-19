package com.github.hongkaiwen.vertx_play;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BatchInsertVerticle extends AbstractVerticle {

  private JDBCClient client;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // creates the jdbc client.
    client = JDBCClient.createNonShared(vertx, config());

    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);

    vertx.setPeriodic(3000, l -> {
      System.out.println(addCount  + "   " + insertCount  + "      " + responseCount);
    });

    vertx.setPeriodic(10, l -> {
      List<Handler<AsyncResult>> thisStep = queue;
      queue = new ArrayList<>(10000);
      if(thisStep.size() == 0){
        return;
      }

      insertCount = insertCount + thisStep.size();

      String collect = thisStep.stream().map(t -> "(1000)").collect(Collectors.joining(","));

      client.query("INSERT INTO metering_record(quantity) VALUES " + collect, event -> {
        if (event.succeeded()) {
          thisStep.stream().forEach(t -> t.handle(Future.succeededFuture()));
        } else {
          thisStep.stream().forEach(t -> t.handle(Future.failedFuture(event.cause())));
        }
      });

    });

    router.get("/insert").handler(request -> {
      batchInsert(event -> {
        if(event.failed()){
          event.cause().printStackTrace();
        }
        responseCount = responseCount + 1;
        request.response().end(String.format("%b", event.succeeded()));
      });
    }).failureHandler(t -> {
      t.failure().printStackTrace();
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

  private List<Handler<AsyncResult>> queue = new ArrayList<>(10000);

  private void batchInsert(Handler<AsyncResult> handler){
    queue.add(handler);
    addCount = addCount + 1;
  }

  int addCount = 0;
  int insertCount = 0;
  int responseCount = 0;


}
