package io.ray.streaming.api.function.impl.window;

import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.AbstractRichFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.common.tuple.Tuple1;

/**
 * Apply reduce function for key window.
 *
 * @param <K> key type
 * @param <T> data type
 * @param <R> result type
 * @param <W> window type
 */
public class ReduceApplyKeyWindowFunction<K, T, R, W extends Window> extends AbstractRichFunction
    implements WindowFunction<K, T, R, W> {

  protected final ReduceFunction<T> reduceFunction;
  private transient KeyValueState<Window, Tuple1> reduceState;

  public ReduceApplyKeyWindowFunction(ReduceFunction<T> reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  @Override
  public void open(RuntimeContext runtimeContext) {
    super.open(runtimeContext);
    reduceState =
        getRuntimeContext()
            .getKeyValueState(
                KeyValueStateDescriptor.build(
                    getRuntimeContext().getOperatorId() + "_reduceApply",
                    Window.class,
                    Tuple1.class));
  }

  @Override
  public R apply(K key, Iterable<T> values, W window) throws Exception {
    Tuple1<T> tuple = reduceState.get(window);

    if (values != null) {
      for (T value : values) {
        if (tuple == null) {
          tuple = Tuple1.of(value);
          continue;
        }
        tuple.setF0(reduceFunction.reduce(tuple.getF0(), value));
      }
    } else if (tuple == null) {
      return null;
    }

    this.reduceState.put(window, tuple);
    return (R) tuple.getF0();
  }
}
