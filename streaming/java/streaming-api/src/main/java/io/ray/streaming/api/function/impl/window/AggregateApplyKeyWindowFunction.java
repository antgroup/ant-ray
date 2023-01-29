package io.ray.streaming.api.function.impl.window;

import io.ray.state.api.KeyValueState;
import io.ray.state.api.KeyValueStateDescriptor;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.AbstractRichFunction;
import io.ray.streaming.api.function.impl.AggregateFunction;
import io.ray.streaming.api.window.Window;
import io.ray.streaming.common.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apply aggregate function for key window.
 *
 * @param <K> key type
 * @param <T> data type
 * @param <A> acc type
 * @param <R> result type
 * @param <W> window type
 */
public class AggregateApplyKeyWindowFunction<K, T, A, R, W extends Window>
    extends AbstractRichFunction implements WindowFunction<K, T, R, W> {

  private static final Logger LOG = LoggerFactory.getLogger(AggregateApplyKeyWindowFunction.class);

  protected final AggregateFunction<T, A, R> aggFunction;
  private transient KeyValueState<Window, Tuple2> aggregateState;

  public AggregateApplyKeyWindowFunction(AggregateFunction<T, A, R> aggFunction) {
    this.aggFunction = aggFunction;
  }

  @Override
  public void open(RuntimeContext runtimeContext) {
    super.open(runtimeContext);
    aggregateState =
        getRuntimeContext()
            .getKeyValueState(
                KeyValueStateDescriptor.build(
                    getRuntimeContext().getOperatorId() + "_aggregateApply",
                    Window.class,
                    Tuple2.class));
  }

  @Override
  public R apply(K key, Iterable<T> values, W window) throws Exception {
    A acc = this.aggFunction.createAccumulator();

    if (values != null) {
      for (T value : values) {
        acc = aggFunction.add(value, acc);
      }
    }

    Tuple2<A, R> tuple2 = aggregateState.get(window);
    if (tuple2 == null) {
      tuple2 = Tuple2.of(acc, aggFunction.getResult(acc));
    }

    this.aggregateState.put(window, tuple2);
    return tuple2.getF1();
  }
}
