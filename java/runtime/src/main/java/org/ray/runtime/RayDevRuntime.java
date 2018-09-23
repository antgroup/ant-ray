package org.ray.runtime;

import org.ray.runtime.config.RayConfig;
import org.ray.runtime.functionmanager.LocalFunctionManager;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.MockRayletClient;

public class RayDevRuntime extends AbstractRayRuntime {

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(rayConfig.driverId);
    this.functions = new LocalFunctionManager(rfm);
    MockObjectStore store = new MockObjectStore(this);
    objectStoreProxy = new ObjectStoreProxy(this, store);
    rayletClient = new MockRayletClient(this, store);
  }

  @Override
  public void shutdown() {
    // nothing to do
  }
}
