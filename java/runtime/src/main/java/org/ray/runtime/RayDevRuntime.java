package org.ray.runtime;

import org.ray.api.config.PathConfig;
import org.ray.api.config.RayConfig;
import org.ray.api.config.RayParameters;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start(RayConfig initConfig) {
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(params.driver_id);
    MockObjectStore store = new MockObjectStore();
    MockRayletClient scheduler = new MockRayletClient(this, store);
    init(scheduler, store, rfm, pathConfig);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
