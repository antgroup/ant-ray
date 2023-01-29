package io.ray.streaming.api.context;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.client.JobClient;
import io.ray.streaming.common.config.CommonConfig;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobGraphBuilder;
import io.ray.streaming.jobgraph.JobGraphOptimizer;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.jobgraph.PlanGraphvizGenerator;
import io.ray.streaming.jobgraph.PlanJSONGenerator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encapsulate the context information of a streaming Job. */
public class StreamingContext implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingContext.class);

  private transient AtomicInteger idGenerator;
  private JobClient jobClient;

  /** The sinks of this streaming job. */
  private List<StreamSink> streamSinks;

  /** The independent operators of this streaming job. */
  private Set<IndependentOperatorDescriptor> independentOperatorDescriptors;

  /** The user custom streaming job configuration. */
  private Map<String, String> jobConfig;

  /** The logic plan. */
  private JobGraph jobGraph;

  private JobGraph originalJobGraph;
  private String jobName;

  private StreamingContext() {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
    this.independentOperatorDescriptors = new HashSet<>();
    this.jobConfig = new HashMap<>();
  }

  public static StreamingContext buildContext() {
    return new StreamingContext();
  }

  /** Construct job DAG, and execute the job. */
  public void execute(String jobName) {
    this.jobName = jobName;
    Preconditions.checkState(!this.streamSinks.isEmpty(), "Need at least one sink to run");
    JobGraphBuilder jobGraphBuilder =
        new JobGraphBuilder(this.streamSinks, this.independentOperatorDescriptors, jobName);
    originalJobGraph = jobGraphBuilder.build();
    LOG.info("========================= Print original job graph start =========================");
    originalJobGraph.printJobGraph();
    LOG.info("========================= Print original job graph end =========================");

    this.jobGraph = new JobGraphOptimizer(originalJobGraph).optimize();
    LOG.info(
        "========================= Print optimized job graph start ========================= ");
    jobGraph.printJobGraph();
    LOG.info("========================= Print optimized job graph end ========================= ");

    LOG.info("JobGraph digraph\n{}", jobGraph.generateDigraph());

    if (!Ray.isInitialized()) {
      if (CommonConfig.MEMORY_CHANNEL.equalsIgnoreCase(jobConfig.get(CommonConfig.CHANNEL_TYPE))) {
        Preconditions.checkArgument(!jobGraph.isCrossLanguageGraph());
        ClusterStarter.startCluster(true);
        LOG.info("Created local cluster for job {}.", jobName);
      } else {
        ClusterStarter.startCluster(false);
        LOG.info("Created multi process cluster for job {}.", jobName);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(StreamingContext.this::stop));
    } else {
      LOG.info("Reuse existing cluster.");
    }

    ServiceLoader<JobClient> serviceLoader = ServiceLoader.load(JobClient.class);
    Iterator<JobClient> iterator = serviceLoader.iterator();
    Preconditions.checkArgument(
        iterator.hasNext(), "No JobClient implementation has been provided.");
    jobClient = iterator.next();
    jobClient.submitJob(jobGraph, jobConfig);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }

  public List<StreamSink> getStreamSinks() {
    return streamSinks;
  }

  public void withConfig(Map<String, String> jobConfig) {
    if (null != this.jobConfig) {
      this.jobConfig.putAll(jobConfig);
    } else {
      this.jobConfig = jobConfig;
    }
  }

  public IndependentOperatorDescriptor withIndependentOperator(String className) {
    return withIndependentOperator(className, "", Language.JAVA);
  }

  public IndependentOperatorDescriptor withIndependentOperator(
      String className, String moduleName) {
    return withIndependentOperator(className, moduleName, Language.PYTHON);
  }

  public IndependentOperatorDescriptor withIndependentOperator(
      String className, String moduleName, Language language) {
    IndependentOperatorDescriptor independentOperatorDescriptor =
        new IndependentOperatorDescriptor(className, moduleName, language);
    this.independentOperatorDescriptors.add(independentOperatorDescriptor);
    return independentOperatorDescriptor;
  }

  public void withIndependentOperators(
      Set<IndependentOperatorDescriptor> independentOperatorDescriptors) {
    this.independentOperatorDescriptors = independentOperatorDescriptors;
  }

  public Set<IndependentOperatorDescriptor> getIndependentOperators() {
    return this.independentOperatorDescriptors;
  }

  private JobGraph getLogicalJobGraph() {
    Preconditions.checkState(!this.streamSinks.isEmpty(), "Need at least one sink to run");
    if (StringUtils.isBlank(jobName)) {
      jobName = "defaultJobName";
    }
    JobGraphBuilder jobGraphBuilder =
        new JobGraphBuilder(this.streamSinks, this.independentOperatorDescriptors, jobName);
    originalJobGraph = jobGraphBuilder.build();
    return originalJobGraph;
  }

  private JobGraph getPhysicalGraph() {
    jobGraph = new JobGraphOptimizer(getLogicalJobGraph()).optimize();
    return jobGraph;
  }

  public String getLogicalGraphviz() {
    return new PlanGraphvizGenerator(getLogicalJobGraph()).getGraphviz();
  }

  public String getPhysicalGraphviz() {
    return new PlanGraphvizGenerator(getPhysicalGraph()).getGraphviz();
  }

  public String getPhysicalPlanJson() {
    return new PlanJSONGenerator(getPhysicalGraph()).getJsonString();
  }

  public void stop() {
    if (Ray.isInitialized()) {
      ClusterStarter.stopCluster();
    }
  }

  public boolean isJobFinished() {
    return jobClient.isJobFinished();
  }

  public boolean shutdownAllWorkers() {
    return jobClient.shutdownAllWorkers();
  }

  public Long getLastValidCheckpointId() {
    return jobClient.getLastValidCheckpointId();
  }

  public ActorHandle getJobMaster() {
    return jobClient.getJobMaster();
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }
}
