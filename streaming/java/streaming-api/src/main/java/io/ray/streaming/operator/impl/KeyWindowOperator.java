package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.window.WindowFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.api.window.assigner.WindowAssigner;
import io.ray.streaming.api.window.trigger.Trigger;
import io.ray.streaming.operator.AbstractWindowOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic operator for key window.
 *
 * @param <K> type of data key
 * @param <T> type of the input
 * @param <R> type of the result
 * @param <W> type of the window
 */
public class KeyWindowOperator<K, T, R, W extends Window>
    extends AbstractWindowOperator<K, T, R, W, WindowFunction<K, T, R, W>> {

  private static final Logger LOG = LoggerFactory.getLogger(KeyWindowOperator.class);

  public KeyWindowOperator(
      WindowFunction<K, T, R, W> function,
      WindowAssigner<K, T, W> windowAssigner,
      Trigger<K, T, W> trigger) {
    super(function, windowAssigner, trigger);
    Type outputType =
        TypeUtils.resolveCollectorValueType(
            TypeUtils.getFunctionMethod(WindowFunction.class, function, "apply")
                .getGenericParameterTypes()[2]);
    typeInfo = new TypeInfo<R>(outputType);
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }

  @Override
  public void finish(long checkpointId) {
    // TODO
  }
}
