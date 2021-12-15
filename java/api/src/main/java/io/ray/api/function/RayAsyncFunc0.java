package io.ray.api.function;

import java.util.concurrent.Future;

@FunctionalInterface
public interface RayAsyncFunc0<A, R> extends RayFuncR<R> {

  Future<R> apply(A a) throws Exception;
}
