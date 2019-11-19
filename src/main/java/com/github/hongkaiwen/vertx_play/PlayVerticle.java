package com.github.hongkaiwen.vertx_play;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

public class PlayVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    vertx.eventBus().consumer("hello", h -> {
        System.out.println(h.body());
        h.reply("haha");
      }
    );

    vertx.eventBus().request("hello", "haha", t -> System.out.println(t.result().body()));
    System.out.println("started");
  }
}
