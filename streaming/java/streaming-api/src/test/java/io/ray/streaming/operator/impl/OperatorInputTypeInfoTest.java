package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.async.AsyncFunction;
import io.ray.streaming.api.function.async.ResultFuture;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.function.impl.ProcessFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.operator.async.AsyncOperator;
import io.ray.streaming.operator.async.AsyncQueue;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeResolver;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

// Lambda doesn't reserve generics type, we need to use inner class to capture generics info.
public class OperatorInputTypeInfoTest {

  @Test
  public void testFilterOperatorType() {
    FilterOperator<String> operator =
        new FilterOperator<>(
            new FilterFunction<String>() {
              @Override
              public boolean filter(String value) {
                return false;
              }
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<String>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testMapOperatorType() {
    MapOperator<String, List<String>> operator =
        new MapOperator<>(
            new MapFunction<String, List<String>>() {
              @Override
              public List<String> map(String value) {
                return null;
              }
            });

    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<List<String>>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testFlatMapOperatorType() {
    FlatMapOperator<String, List<String>> operator =
        new FlatMapOperator<>(
            new FlatMapFunction<String, List<String>>() {
              @Override
              public void flatMap(String value, Collector<List<String>> collector) {}
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<List<String>>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testKeyByOperator() {
    KeyByOperator<String, String> operator =
        new KeyByOperator<>(
            new KeyFunction<String, String>() {
              @Override
              public String keyBy(String value) {
                return null;
              }
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<String>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testProcessOperatorType() {
    ProcessOperator<String, List<String>> operator =
        new ProcessOperator<>(
            new ProcessFunction<String, List<String>>() {

              @Override
              public void process(String value, Collector<List<String>> collector) {}
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<List<String>>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testReduceOperatorType() {
    Class<?>[] reduceTypes = TypeResolver.resolveRawArguments(ReduceFunction.class, String.class);
    ReduceOperator<String, String> operator =
        new ReduceOperator<>(
            new ReduceFunction<String>() {
              @Override
              public String reduce(String oldValue, String newValue) {
                return null;
              }
            },
            reduceTypes);
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<String>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testSinkOperatorType() {
    SinkOperator<String> operator =
        new SinkOperator<>(
            new SinkFunction<String>() {
              @Override
              public void sink(String value) {}
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<String>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testSourceOperatorImplType() {
    SourceOperator<String> operator =
        new SourceOperator<>(
            new SourceFunction<String>() {
              @Override
              public void init(int parallel, int index) {}

              @Override
              public void fetch(long checkpointId, SourceContext<String> ctx) throws Exception {}

              @Override
              public void close() {}
            });
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<String>() {});
  }

  @Test
  public void testAsyncOperatorType() {
    AsyncOperator<String, Integer> operator =
        new AsyncOperator<>(
            new AsyncFunction<String, Integer>() {
              @Override
              public void open(RuntimeContext runtimeContext) {}

              @Override
              public void close() {}

              @Override
              public void runAsync(String input, ResultFuture<Integer> resultFuture) {}
            },
            AsyncQueue.OrderType.ORDERED,
            10,
            10);
    Assert.assertEquals(operator.getTypeInfo(), new TypeInfo<Integer>() {});
    Assert.assertEquals(operator.getInputTypeInfo(), new TypeInfo<String>() {});
  }
}
