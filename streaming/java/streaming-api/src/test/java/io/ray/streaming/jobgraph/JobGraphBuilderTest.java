package io.ray.streaming.jobgraph;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.fury.types.TypeInference;
import io.ray.streaming.DefaultRuntimeContext;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.IndependentOperatorDescriptor;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.partition.impl.ForwardPartition;
import io.ray.streaming.api.partition.impl.KeyPartition;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.DataStreamSink;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.api.stream.KeyDataStream;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.api.stream.StreamTest;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.impl.SinkOperator;
import io.ray.streaming.operator.proxy.OneInputOperatorProxy;
import io.ray.streaming.operator.proxy.OperatorProxy;
import io.ray.streaming.operator.proxy.OutputOperatorProxy;
import io.ray.streaming.operator.proxy.SourceOperatorProxy;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonKeyDataStream;
import io.ray.streaming.python.stream.PythonStreamSink;
import io.ray.streaming.util.EndOfDataException;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JobGraphBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(JobGraphBuilderTest.class);

  @Test
  public void testDataSync() {
    JobGraph jobGraph = buildDataSyncJobGraph();
    List<JobVertex> jobVertexList = jobGraph.getJobVertices();
    List<JobEdge> jobEdgeList = jobGraph.getJobEdges();

    Assert.assertEquals(jobVertexList.size(), 2);
    Assert.assertEquals(jobEdgeList.size(), 1);

    JobEdge jobEdge = jobEdgeList.get(0);
    Assert.assertEquals(jobEdge.getPartition().getClass(), ForwardPartition.class);

    JobVertex sinkVertex = jobVertexList.get(1);
    JobVertex sourceVertex = jobVertexList.get(0);
    Assert.assertEquals(sinkVertex.getVertexType(), VertexType.sink);
    Assert.assertEquals(sourceVertex.getVertexType(), VertexType.source);
  }

  public JobGraph buildDataSyncJobGraph() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream =
        DataStreamSource.fromCollection(streamingContext, Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.build();
    return jobGraph;
  }

  @Test
  public void testKeyByJobGraph() {
    JobGraph jobGraph = buildKeyByJobGraph();
    List<JobVertex> jobVertexList = jobGraph.getJobVertices();
    List<JobEdge> jobEdgeList = jobGraph.getJobEdges();

    Assert.assertEquals(jobVertexList.size(), 3);
    Assert.assertEquals(jobEdgeList.size(), 2);

    JobVertex source = jobVertexList.get(0);
    JobVertex map = jobVertexList.get(1);
    JobVertex sink = jobVertexList.get(2);

    Assert.assertEquals(source.getVertexType(), VertexType.source);
    Assert.assertEquals(map.getVertexType(), VertexType.process);
    Assert.assertEquals(sink.getVertexType(), VertexType.sink);

    JobEdge keyBy2Sink = jobEdgeList.get(0);
    JobEdge source2KeyBy = jobEdgeList.get(1);

    Assert.assertEquals(keyBy2Sink.getPartition().getClass(), KeyPartition.class);
    Assert.assertEquals(source2KeyBy.getPartition().getClass(), ForwardPartition.class);
  }

  public JobGraph buildKeyByJobGraph() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream =
        DataStreamSource.fromCollection(streamingContext, Lists.newArrayList("1", "2", "3", "4"));
    StreamSink streamSink = dataStream.keyBy(x -> x).sink(x -> LOG.info(x));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.build();
    return jobGraph;
  }

  @Test
  public void testHybridStreamSchema() {
    DataStream<Integer> dataStream =
        DataStreamSource.fromCollection(
            StreamingContext.buildContext(), Lists.newArrayList(1, 2, 3));
    PythonKeyDataStream keyedStream =
        dataStream
            .asPythonStream()
            .map(new PythonFunction("m", "map"))
            .keyBy(new PythonFunction("m", "keyBy"));
    DataStream<StreamTest.B> reducedStream =
        keyedStream
            .asJavaStream()
            .asStream(new TypeInfo<KeyDataStream<String, StreamTest.B>>() {})
            .reduce((b1, b2) -> new StreamTest.B());
    PythonStreamSink sink = reducedStream.asPythonStream().sink(new PythonFunction("m", "sink"));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(sink));
    jobGraphBuilder.build();
    Assert.assertEquals(keyedStream.getSchema(), TypeInference.inferSchema(StreamTest.B.class));
    Assert.assertEquals(reducedStream.getSchema(), TypeInference.inferSchema(StreamTest.B.class));
  }

  @Test
  public void testJobGraphByStreamWithIndependentActors() {
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

    DataStream<String> dataStream =
        DataStreamSource.fromCollection(streamingContext, Lists.newArrayList("a", "b", "c"));
    streamingContext.addSink(dataStream.sink(LOG::info));

    JobGraphBuilder jobGraphBuilder =
        new JobGraphBuilder(
            streamingContext.getStreamSinks(),
            streamingContext.getIndependentOperators(),
            "testStreamWithIndependentActors");
    JobGraph jobGraph = jobGraphBuilder.build();

    Assert.assertEquals(jobGraph.getJobVertices().size(), 2);
    Assert.assertEquals(jobGraph.getJobEdges().size(), 1);
    Assert.assertEquals(jobGraph.getIndependentOperators().size(), 2);
    for (IndependentOperatorDescriptor independentOperatorDescriptor :
        jobGraph.getIndependentOperators()) {
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

    String digraph = jobGraph.generateDigraph();
    Assert.assertTrue(digraph.contains(new IndependentOperatorDescriptor(className).getName()));
    Assert.assertTrue(
        digraph.contains(new IndependentOperatorDescriptor(className, moduleName).getName()));
  }

  public static class WordCount {
    String word;
    int count;

    public WordCount(String word, int count) {
      this.word = word;
      this.count = count;
    }
  }

  @Test
  public void testHybridDataStreamSerialization() {
    ArrayList<WordCount> sourceData =
        Lists.newArrayList(
            new WordCount("str", 1), new WordCount("str", 1), new WordCount("str", 1));
    List<WordCount> sinkResult = new ArrayList<>();
    DataStream<WordCount> dataStream =
        DataStreamSource.fromCollection(StreamingContext.buildContext(), sourceData)
            .withType(new TypeInfo<WordCount>() {});
    PythonDataStream mapStream = dataStream.asPythonStream().map(new PythonFunction("m", "map"));
    DataStreamSink<WordCount> sink =
        mapStream
            .asJavaStream()
            .asStream(new TypeInfo<DataStream<WordCount>>() {})
            .sink(sinkResult::add);
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(sink));
    JobGraph jobGraph = jobGraphBuilder.build();
    Assert.assertEquals(dataStream.getSchema(), TypeInference.inferSchema(WordCount.class));
    Assert.assertEquals(mapStream.getSchema(), TypeInference.inferSchema(WordCount.class));
    Assert.assertEquals(sink.getType().getType(), WordCount.class);
    Encoder<WordCount> encoder = Encoders.bean(WordCount.class);

    List<byte[]> out = new ArrayList<>();
    // test encode data
    {
      SourceOperatorProxy operator =
          (SourceOperatorProxy)
              jobGraph.getJobVertices().stream()
                  .map(JobVertex::getOperator)
                  .filter(
                      o ->
                          o instanceof OutputOperatorProxy
                              && o.getOpType() == OperatorInputType.SOURCE)
                  .findAny()
                  .orElseThrow(RuntimeException::new);
      List<Collector> collectors =
          Collections.singletonList(
              new Collector<Record>() {
                @Override
                public void collect(Record value) {
                  out.add((byte[]) value.getValue());
                }

                @Override
                public void retract(Record value) {
                  collect(value);
                }
              });
      operator.open(collectors, new DefaultRuntimeContext());
      try {
        operator.fetch(0);
      } catch (EndOfDataException e) {
      }
      List<byte[]> sourceRows =
          sourceData.stream().map(x -> encoder.toRow(x).toBytes()).collect(Collectors.toList());
      Assert.assertEquals(sourceRows.size(), out.size());
      for (int i = 0; i < out.size(); i++) {
        Assert.assertTrue(Arrays.equals(sourceRows.get(i), (byte[]) out.get(i)));
      }
    }

    // test decode data
    {
      OneInputOperatorProxy operator =
          (OneInputOperatorProxy)
              jobGraph.getJobVertices().stream()
                  .map(JobVertex::getOperator)
                  .filter(
                      o ->
                          o instanceof OperatorProxy
                              && ((OperatorProxy) o).getOriginalOperator() instanceof SinkOperator)
                  .findAny()
                  .orElseThrow(RuntimeException::new);
      operator.open(new ArrayList<Collector>(), new DefaultRuntimeContext());
      out.forEach(
          bytes -> {
            try {
              operator.processElement(new Record<>(bytes));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      Assert.assertEquals(sinkResult.size(), sourceData.size());
      for (int i = 0; i < sinkResult.size(); i++) {
        Assert.assertEquals(sinkResult.get(i).word, sourceData.get(i).word);
        Assert.assertEquals(sinkResult.get(i).count, sourceData.get(i).count);
      }
    }
  }

  @Test
  public void testJobGraphViz() {
    JobGraph jobGraph = buildKeyByJobGraph();
    jobGraph.generateDigraph();
    String diGraph = jobGraph.getDigraph();
    LOG.info(diGraph);
    Assert.assertTrue(diGraph.contains("\"1-SourceOperator\" -> \"2-KeyByOperator\""));
    Assert.assertTrue(diGraph.contains("\"2-KeyByOperator\" -> \"3-SinkOperator\""));
  }

  @Test
  public void testAddEdgeUnique() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream =
        DataStreamSource.fromCollection(streamingContext, Lists.newArrayList("a", "b", "c"))
            .map((MapFunction<String, String>) value -> value);
    DataStream<String> stream1 =
        dataStream.flatMap(
            (FlatMapFunction<String, String>) (value, collector) -> collector.collect(value));
    DataStream<String> stream2 =
        dataStream.flatMap(
            (FlatMapFunction<String, String>) (value, collector) -> collector.collect(value));

    StreamSink streamSink = stream1.union(stream2).sink(x -> {});

    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.build();
    Assert.assertEquals(jobGraph.getJobEdges().size(), 6);
  }
}
