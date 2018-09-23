package org.ray.api.test;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.ray.api.Ray;

public class TestListener extends RunListener {

  @Override
  public void testRunStarted(Description description) {
//    String workDir = System.getProperty("user.dir");
    System.setProperty("ray.home", "/Users/haochen/code/ant_ray/");
    Ray.init();
  }

  @Override
  public void testRunFinished(Result result) {
    Ray.shutdown();
  }
}
