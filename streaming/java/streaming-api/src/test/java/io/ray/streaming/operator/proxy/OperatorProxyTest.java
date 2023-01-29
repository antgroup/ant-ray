package io.ray.streaming.operator.proxy;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.stream.StreamTest.A;
import io.ray.streaming.api.stream.StreamTest.B;
import io.ray.streaming.operator.impl.JoinOperator;
import io.ray.streaming.operator.impl.MapOperator;
import io.ray.streaming.operator.impl.SourceOperator;
import io.ray.streaming.util.TypeInfo;
import org.testng.annotations.Test;

public class OperatorProxyTest {

  @Test
  public void testSourceOperatorProxy() {
    SourceOperator<A> operator =
        new SourceOperator<>(
            new SourceFunction<A>() {
              @Override
              public void init(int parallel, int index) {}

              @Override
              public void fetch(long checkpointId, SourceContext<A> ctx) throws Exception {}

              @Override
              public void close() {}
            });
    SourceOperatorProxy proxy = new SourceOperatorProxy(operator);
    assertSame(proxy.getOriginalOperator(), operator);
    assertSame(proxy.getSourceContext(), operator.getSourceContext());
    assertEquals(proxy.getTypeInfo(), operator.getTypeInfo());
    assertEquals(proxy.getSchema(), operator.getSchema());
  }

  @Test
  public void testOneInputOperatorProxy() {
    MapOperator<A, B> operator =
        new MapOperator<>(
            new MapFunction<A, B>() {
              @Override
              public B map(A value) {
                return null;
              }
            });
    OneInputOperatorProxy proxy = new OneInputOperatorProxy(operator);
    assertSame(proxy.getOriginalOperator(), operator);
    assertEquals(proxy.getTypeInfo(), operator.getTypeInfo());
    assertEquals(proxy.getSchema(), operator.getSchema());
    assertEquals(proxy.getInputTypeInfo(), operator.getInputTypeInfo());
  }

  @Test
  public void testTwoInputOperatorProxy() {
    JoinOperator operator = new JoinOperator();
    TwoInputOperatorProxy proxy = new TwoInputOperatorProxy(operator);
    assertSame(proxy.getOriginalOperator(), operator);
    proxy.setLeftInputTypeInfo(new TypeInfo<A>() {});
    proxy.setRightInputTypeInfo(new TypeInfo<B>() {});
    proxy.setTypeInfo(new TypeInfo<A>() {});
    assertEquals(proxy.getTypeInfo().getType(), A.class);
    assertEquals(proxy.getLeftInputTypeInfo().getType(), A.class);
    assertEquals(proxy.getRightInputTypeInfo().getType(), B.class);
  }
}
