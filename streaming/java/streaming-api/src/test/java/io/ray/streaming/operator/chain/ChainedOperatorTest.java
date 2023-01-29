package io.ray.streaming.operator.chain;

import static io.ray.streaming.api.stream.StreamTest.A;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.ray.fury.types.TypeInference;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.operator.impl.FlatMapOperator;
import io.ray.streaming.operator.impl.MapOperator;
import io.ray.streaming.util.OperatorUtil;
import io.ray.streaming.util.TypeInfo;
import java.util.Arrays;
import java.util.HashMap;
import org.testng.annotations.Test;

public class ChainedOperatorTest {

  private ChainedOneInputOperator createChainedOneInputOperator() {
    MapOperator<A, String> operator1 =
        new MapOperator<>(
            new MapFunction<A, String>() {
              @Override
              public String map(A value) {
                return null;
              }
            });
    FlatMapOperator<String, A> operator2 =
        new FlatMapOperator<>(
            new FlatMapFunction<String, A>() {
              @Override
              public void flatMap(String value, Collector<A> collector) {}
            });
    operator1.addNextOperator(operator2);
    return (ChainedOneInputOperator)
        OperatorUtil.newChainedOperator(
            Arrays.asList(operator1, operator2),
            ImmutableList.of(new HashMap<>(), new HashMap<>()),
            ImmutableList.of(new HashMap<>(), new HashMap<>()));
  }

  @Test
  public void testTypeInfo() {
    ChainedOneInputOperator operator = createChainedOneInputOperator();
    assertTrue(operator.hasTypeInfo());
    assertEquals(operator.getTypeInfo(), new TypeInfo<A>() {});
    assertEquals(operator.getInputTypeInfo(), new TypeInfo<A>() {});
  }

  @Test
  public void testSchema() {
    ChainedOperator operator = createChainedOneInputOperator();
    assertTrue(operator.hasTypeInfo());
    operator.setSchema(TypeInference.inferSchema(operator.getTypeInfo().getType()));
    assertTrue(operator.hasSchema());
    assertEquals(operator.getSchema(), operator.getSchema());
  }
}
