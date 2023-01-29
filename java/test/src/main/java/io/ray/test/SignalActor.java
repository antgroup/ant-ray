package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalActor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SignalActor.class);

  private Semaphore semaphore;

  public SignalActor() {
    this.semaphore = new Semaphore(0);
  }

  public SignalActor(int permits) {
    this.semaphore = new Semaphore(permits);
  }

  public int sendSignal() {
    LOGGER.info("sendSignal called.");
    this.semaphore.release();
    LOGGER.info("sendSignal called done.");
    return 0;
  }

  public int waitSignal() throws InterruptedException {
    LOGGER.info("waitSignal called.");
    this.semaphore.acquire();
    LOGGER.info("waitSignal called done.");
    return 0;
  }

  public Integer ping() {
    return 0;
  }

  public static ActorHandle<SignalActor> create() {
    return Ray.actor(SignalActor::new).setMaxConcurrency(2).remote();
  }
}
