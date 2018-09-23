package org.ray.api.test;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.ray.api.Ray;

public class TestListener extends RunListener {

  @Override
  public void testRunStarted(Description description) {
    String workDir = System.getProperty("user.dir");
    String rayHome = workDir.substring(0, workDir.length() - "java/test".length());
    System.setProperty("ray.home", rayHome);
    Ray.init();
  }

  @Override
  public void testRunFinished(Result result) {
    Ray.shutdown();
  }
}
