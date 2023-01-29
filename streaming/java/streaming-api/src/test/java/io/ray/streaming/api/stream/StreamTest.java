package io.ray.streaming.api.stream;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.ray.fury.types.TypeInference;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.IndependentOperatorDescriptor;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.impl.ArrowOperator;
import io.ray.streaming.operator.impl.MapOperator;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonKeyDataStream;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class StreamTest {

  @Test
  public void testReferencedDataStream() {
    DataStream dataStream =
        new DataStream(StreamingContext.buildContext(), new MapOperator(value -> null));
    PythonDataStream pythonDataStream = dataStream.asPythonStream();
    DataStream javaStream = pythonDataStream.asJavaStream();
    assertEquals(dataStream.getId(), pythonDataStream.getId());
    assertEquals(dataStream.getId(), javaStream.getId());
    javaStream.setParallelism(10);
    assertEquals(dataStream.getParallelism(), pythonDataStream.getParallelism());
    assertEquals(dataStream.getParallelism(), javaStream.getParallelism());
  }

  @Test
  public void testReferencedKeyDataStream() {
    DataStream dataStream =
        new DataStream(StreamingContext.buildContext(), new MapOperator(value -> null));
    KeyDataStream keyDataStream = dataStream.keyBy(value -> null);
    PythonKeyDataStream pythonKeyDataStream = keyDataStream.asPythonStream();
    KeyDataStream javaKeyDataStream = pythonKeyDataStream.asJavaStream();
    assertEquals(keyDataStream.getId(), pythonKeyDataStream.getId());
    assertEquals(keyDataStream.getId(), javaKeyDataStream.getId());
    javaKeyDataStream.setParallelism(10);
    assertEquals(keyDataStream.getParallelism(), pythonKeyDataStream.getParallelism());
    assertEquals(keyDataStream.getParallelism(), javaKeyDataStream.getParallelism());
  }

  @Test
  public void testStreamTypeInfo() {
    DataStream<String> dataStream =
        DataStreamSource.fromCollection(
                StreamingContext.buildContext(), Lists.newArrayList("a", "b", "c"))
            .withType(new TypeInfo<String>() {});
    assertEquals(dataStream.getType(), new TypeInfo<String>() {});
    DataStream<String> reducedStream =
        dataStream.keyBy(String::length).reduce((v1, v2) -> v1 + "," + v2);
    assertEquals(reducedStream.getType(), new TypeInfo<String>() {});
  }

  public static class A {
    int f1;
    String f2;
  }

  public static class B {
    A f1;
    String f2;
  }

  @Test
  public void testStreamSchema() {
    DataStream<A> dataStream =
        DataStreamSource.fromCollection(
                StreamingContext.buildContext(), Lists.newArrayList(new A(), new A()))
            .withType(new TypeInfo<A>() {});
    assertEquals(dataStream.getType(), new TypeInfo<A>() {});
    assertEquals(dataStream.getSchema(), TypeInference.inferSchema(A.class));

    DataStream<B> reducedStream =
        dataStream
            .flatMap((A a, Collector<B> c) -> c.collect(new B()))
            .keyBy((b) -> b.f1)
            .reduce((v1, v2) -> new B());
    assertEquals(reducedStream.getSchema(), TypeInference.inferSchema(B.class));
  }

  @Test
  public void testUnionStream() {
    DataStream<A> dataStream =
        DataStreamSource.fromCollection(
                StreamingContext.buildContext(), Lists.newArrayList(new A(), new A()))
            .withType(new TypeInfo<A>() {});
    UnionStream unionStream = new UnionStream(dataStream, new ArrayList<>());
    Assert.assertEquals(unionStream.getType(), new TypeInfo<A>() {});
  }

  @Test
  public void testArrowStream() {
    DataStream dataStream =
        (DataStream)
            new DataStream(StreamingContext.buildContext(), new MapOperator(value -> null))
                .withType(new TypeInfo(A.class));
    {
      DataStream arrowStream = dataStream.toArrowStream(10, 1000);
      Assert.assertTrue(arrowStream.getOperator() instanceof ArrowOperator);
      ArrowOperator operator = (ArrowOperator) arrowStream.getOperator();
      Assert.assertEquals(operator.getBatchSize(), 10);
      Assert.assertEquals(operator.getTimeoutMilliseconds(), 1000);
    }
    PythonDataStream pythonDataStream = dataStream.asPythonStream();
    PythonDataStream arrowStream = pythonDataStream.toArrowStream(10, 1000);
    {
      PythonOperator operator = (PythonOperator) arrowStream.getOperator();
      Assert.assertEquals(
          operator.getModuleName(), PythonOperator.OPERATOR_MODULE, "ArrowOperator");
    }
    {
      PythonDataStream pandasStream = pythonDataStream.toPandasStream(10, 1000);
      PythonOperator operator = (PythonOperator) (pandasStream.getInputStream().getOperator());
      Assert.assertEquals(
          operator.getModuleName(), PythonOperator.OPERATOR_MODULE, "ArrowOperator");
    }
    {
      PythonDataStream pandasStream = arrowStream.toPandasStream();
      PythonOperator operator = (PythonOperator) (pandasStream.getInputStream().getOperator());
      Assert.assertEquals(
          operator.getModuleName(), PythonOperator.OPERATOR_MODULE, "ArrowOperator");
      PythonOperator operator2 = (PythonOperator) pandasStream.getOperator();
      Assert.assertEquals(operator2.getFunction().getFunctionName(), "ArrowToPandasFunction");
    }
  }

  @Test
  public void testStreamContextWithIndependentOperators() {
    String className = "testClass";
    String moduleName = "testModule";
    StreamingContext streamingContext = StreamingContext.buildContext();

    streamingContext
        .withIndependentOperator(className)
        .setParallelism(3)
        .setResource(ImmutableMap.of(ResourceKey.CPU.name(), 2D))
        .setConfig(ImmutableMap.of("k1", "v1"));

    streamingContext
        .withIndependentOperator(className, moduleName)
        .setParallelism(2)
        .setResource(ImmutableMap.of(ResourceKey.MEM.name(), 2500D))
        .setConfig(ImmutableMap.of("k2", "v2"))
        .setLazyScheduling();

    // invalid(is duplicated)
    streamingContext
        .withIndependentOperator(className, moduleName)
        .setParallelism(4)
        .setResource(ImmutableMap.of(ResourceKey.MEM.name(), 2600D))
        .setConfig(ImmutableMap.of("k3", "v3"));

    Set<IndependentOperatorDescriptor> result = streamingContext.getIndependentOperators();
    Assert.assertEquals(result.size(), 2);
    for (IndependentOperatorDescriptor independentOperatorDescriptor : result) {
      Assert.assertNotNull(independentOperatorDescriptor);
      if (!StringUtils.isEmpty(independentOperatorDescriptor.getModuleName())) {
        Assert.assertEquals(independentOperatorDescriptor.getModuleName(), moduleName);
        Assert.assertEquals(independentOperatorDescriptor.getClassName(), className);
        Assert.assertEquals(independentOperatorDescriptor.getLanguage(), Language.PYTHON);
        Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 2);
        Assert.assertEquals(
            (double) independentOperatorDescriptor.getResource().get(ResourceKey.MEM.name()),
            2500D);
        Assert.assertEquals(independentOperatorDescriptor.getConfig().get("k2"), "v2");
        Assert.assertTrue(independentOperatorDescriptor.isLazyScheduling());
      } else {
        Assert.assertTrue(independentOperatorDescriptor.getModuleName().isEmpty());
        Assert.assertEquals(independentOperatorDescriptor.getClassName(), className);
        Assert.assertEquals(independentOperatorDescriptor.getLanguage(), Language.JAVA);
        Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 3);
        Assert.assertEquals(
            (double) independentOperatorDescriptor.getResource().get(ResourceKey.CPU.name()), 2D);
        Assert.assertEquals(independentOperatorDescriptor.getConfig().get("k1"), "v1");
        Assert.assertFalse(independentOperatorDescriptor.isLazyScheduling());
      }
    }

    streamingContext = StreamingContext.buildContext();
    Set<IndependentOperatorDescriptor> independentOperatorDescriptors =
        ImmutableSet.of(
            new IndependentOperatorDescriptor(className, moduleName),
            new IndependentOperatorDescriptor(className),
            new IndependentOperatorDescriptor(className + "1", moduleName));
    streamingContext.withIndependentOperators(independentOperatorDescriptors);

    result = streamingContext.getIndependentOperators();
    Assert.assertEquals(result.size(), 3);
    for (IndependentOperatorDescriptor independentOperatorDescriptor : result) {
      Assert.assertNotNull(independentOperatorDescriptor);
      Assert.assertEquals(independentOperatorDescriptor.getParallelism(), 1);
      Assert.assertNotNull(independentOperatorDescriptor.getConfig());
      Assert.assertNotNull(independentOperatorDescriptor.getResource());
      Assert.assertTrue(independentOperatorDescriptor.getConfig().isEmpty());
      Assert.assertTrue(independentOperatorDescriptor.getResource().isEmpty());
    }
  }
}
