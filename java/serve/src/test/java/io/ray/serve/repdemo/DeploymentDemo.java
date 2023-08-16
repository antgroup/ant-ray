package io.ray.serve.repdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.RayServeHandle;

// 1. Deployment and Application
public class DeploymentDemo {
  private String msg;

  public DeploymentDemo(String msg) {
    this.msg = msg;
  }

  public String call() {
    return msg;
  }

  public static void main(String[] args) {
    Deployment deployment =
        Serve.deployment()
            .setDeploymentDef(DeploymentDemo.class.getName())
            .create();
    Application strategyApp = deployment.bind();
    RayServeHandle handle = Serve.run(strategyApp);
    System.out.println(handle.remote().get());
  }
}
