package org.ray.runtime;

import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  @Override
  public void start() {
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(rayConfig.driverId);
    MockObjectStore store = new MockObjectStore();
    MockRayletClient scheduler = new MockRayletClient(this, store);
    initMembers(scheduler, store, rfm);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
