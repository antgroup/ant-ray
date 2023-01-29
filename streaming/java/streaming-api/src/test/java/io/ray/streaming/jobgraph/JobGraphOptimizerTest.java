package io.ray.streaming.jobgraph;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.Lists;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.python.PythonFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class JobGraphOptimizerTest {
  private static final Logger LOG = LoggerFactory.getLogger(JobGraphOptimizerTest.class);

  /** DataStreamSource: s1, s2, s3 Graph: s1 -> filter -> map -> union(s2) -> join(s3) ->sink */
  @Test
  public void testOptimizeChain() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<Integer> source1 =
        DataStreamSource.fromCollection(context, Lists.newArrayList(1, 2, 3));
    DataStream<String> source2 =
        DataStreamSource.fromCollection(context, Lists.newArrayList("1", "2", "3"));
    DataStream<String> source3 =
        DataStreamSource.fromCollection(context, Lists.newArrayList("2", "3", "4"));
    source1
        .filter(x -> x > 1)
        .map(String::valueOf)
        .union(source2)
        .join(source3)
        .sink(x -> System.out.println("Sink " + x));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 8);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 5);
  }

  /**
   * DataStreamSource: s1 Graph: s2 = s1 -> map | s2 -> filter1 -> sink1(parallel = 2) | s2->
   * filter2 -> sink2(parallel = 2)
   */
  @Test
  public void testOptimizeTree() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<String> source1 =
        DataStreamSource.fromCollection(context, Lists.newArrayList("1", "2", "3"));
    DataStream<Object> source2 = source1.map(String::valueOf);
    source2.filter(x -> (int) x < 2).sink(x -> System.out.println((int) x * 2)).setParallelism(2);
    source2.filter(x -> (int) x < 2).sink(x -> System.out.println((int) x * 3)).setParallelism(2);
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 6);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 3);
  }

  /**
   * DataStreamSource: s1 ｜ Graph: s2 = s1 ->map ｜ s2 -> filter1 -> map(s3, parallel: 2) ｜ s2 ->
   * filter2-> union(s3) -> sink
   */
  @Test
  public void testOptimizeGraph() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<String> s1 =
        DataStreamSource.fromCollection(context, Lists.newArrayList("1", "2", "3"));
    DataStream<Object> s2 = s1.map(String::valueOf);
    DataStream<Object> s3 =
        s2.filter(x -> (int) x < 2).map(x -> (Object) ((int) x * 2)).setParallelism(2);
    s2.filter(x -> (int) x > 2).union(s3).sink(x -> System.out.println((int) x * 3));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 7);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    System.out.println("optimized " + optimizedJobGraph.generateDigraph());
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 3);
  }

  @Test
  public void testWithResourceAfterOptimizeHybridStream() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<Integer> source =
        DataStreamSource.fromCollection(context, Lists.newArrayList(1, 2, 3));
    source
        .asPythonStream()
        .map(pyFunc(1))
        .withResource(ResourceKey.GPU.name(), 0.5)
        .setParallelism(2)
        .map(pyFunc(1))
        .withResource(ResourceKey.GPU.name(), 1.0)
        .setParallelism(1)
        .sink(pyFunc(1));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 4);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 3);
    Double gpuResource =
        optimizedJobGraph.getVertex(3).getOperator().getResource().get(ResourceKey.GPU.name());
    assertNotNull(gpuResource);
    assertEquals(gpuResource.doubleValue(), 1.0);
  }

  @Test
  public void testOptimizeHybridStream() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<Integer> source1 =
        DataStreamSource.fromCollection(context, Lists.newArrayList(1, 2, 3));
    DataStream<String> source2 =
        DataStreamSource.fromCollection(context, Lists.newArrayList("1", "2", "3"));
    source1
        .asPythonStream()
        .map(pyFunc(1))
        .filter(pyFunc(2))
        .union(source2.asPythonStream().filter(pyFunc(3)).map(pyFunc(4)))
        .asJavaStream()
        .sink(x -> System.out.println("Sink " + x));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 8);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 6);
  }

  private PythonFunction pyFunc(int number) {
    return new PythonFunction("module", "func" + number);
  }
}
