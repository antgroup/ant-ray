package io.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.context.IndependentOperatorDescriptor;
import io.ray.streaming.common.serializer.Serializer;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job graph, the logical plan of streaming job. */
public class JobGraph implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobGraph.class);

  private final String jobName;
  private final Map<String, String> jobConfig;
  private List<JobVertex> jobVertices;
  private List<JobEdge> jobEdges;
  private Set<IndependentOperatorDescriptor> independentOperatorDescriptors;
  private String digraph;

  public JobGraph(String jobName, Map<String, String> jobConfig) {
    this(jobName, jobConfig, new ArrayList<>(), new ArrayList<>(), new HashSet<>());
  }

  public JobGraph(
      String jobName,
      Map<String, String> jobConfig,
      List<JobVertex> jobVertices,
      List<JobEdge> jobEdges,
      Set<IndependentOperatorDescriptor> independentOperatorDescriptors) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
    this.jobVertices = jobVertices;
    this.jobEdges = jobEdges;
    this.independentOperatorDescriptors = independentOperatorDescriptors;
    generateDigraph();
  }

  /**
   * Generate direct-graph(made up of a set of vertices and connected by edges) by current job graph
   * for simple log printing.
   *
   * @return Digraph in string type.
   */
  public String generateDigraph() {
    StringBuilder digraph = new StringBuilder();
    String separator = System.getProperty("line.separator");

    // start
    digraph.append("digraph ").append(jobName).append(" ").append(" {");

    // for DAG
    for (JobEdge jobEdge : jobEdges) {
      String srcNode = null;
      String targetNode = null;
      for (JobVertex jobVertex : jobVertices) {
        if (jobEdge.getSourceVertexId() == jobVertex.getVertexId()) {
          srcNode = jobVertex.getVertexId() + "-" + jobVertex.getOperator().getName();
        } else if (jobEdge.getTargetVertexId() == jobVertex.getVertexId()) {
          targetNode = jobVertex.getVertexId() + "-" + jobVertex.getOperator().getName();
        }
      }
      digraph.append(separator);
      digraph.append(String.format("  \"%s\" -> \"%s\"", srcNode, targetNode));
    }

    // for independent operators
    for (IndependentOperatorDescriptor independentOperatorDescriptor :
        independentOperatorDescriptors) {
      digraph.append(separator);
      digraph.append(String.format("  \"%s\"", independentOperatorDescriptor.getName()));
    }

    // end
    digraph.append(separator).append("}");

    this.digraph = digraph.toString();
    return this.digraph;
  }

  public void addIndependentOperator(IndependentOperatorDescriptor independentOperatorDescriptor) {
    this.independentOperatorDescriptors.add(independentOperatorDescriptor);
  }

  public void setIndependentOperators(
      Set<IndependentOperatorDescriptor> independentOperatorDescriptors) {
    this.independentOperatorDescriptors = independentOperatorDescriptors;
  }

  public void addVertex(JobVertex vertex) {
    this.jobVertices.add(vertex);
  }

  public void addEdgeIfNotExist(JobEdge jobEdge) {
    for (JobEdge edge : this.jobEdges) {
      if (jobEdge.getSourceVertexId() == edge.getSourceVertexId()
          && jobEdge.getTargetVertexId() == edge.getTargetVertexId()) {
        return;
      }
    }
    this.jobEdges.add(jobEdge);
  }

  public List<JobVertex> getJobVertices() {
    return jobVertices;
  }

  public List<JobVertex> getSourceVertices() {
    return jobVertices.stream()
        .filter(v -> v.getVertexType() == VertexType.source)
        .collect(Collectors.toList());
  }

  public List<JobVertex> getSinkVertices() {
    return jobVertices.stream()
        .filter(v -> v.getVertexType() == VertexType.sink)
        .collect(Collectors.toList());
  }

  public JobVertex getVertex(int vertexId) {
    return jobVertices.stream().filter(v -> v.getVertexId() == vertexId).findFirst().get();
  }

  public boolean containJobVertex(int vertexId) {
    for (JobVertex jobVertex : jobVertices) {
      if (jobVertex.getVertexId() == vertexId) {
        return true;
      }
    }
    return false;
  }

  public List<JobEdge> getJobEdges() {
    return jobEdges;
  }

  public Set<JobEdge> getVertexInputEdges(int vertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getTargetVertexId() == vertexId)
        .collect(Collectors.toSet());
  }

  public Set<JobEdge> getVertexOutputEdges(int vertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getSourceVertexId() == vertexId)
        .collect(Collectors.toSet());
  }

  public String getDigraph() {
    return digraph;
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public void overrideJobConfig(Map<String, String> config) {
    jobConfig.putAll(config);
  }

  public Set<IndependentOperatorDescriptor> getIndependentOperators() {
    return this.independentOperatorDescriptors;
  }

  public void printJobGraph() {
    if (!LOG.isInfoEnabled()) {
      return;
    }
    LOG.info("Printing job graph:");
    for (JobVertex jobVertex : jobVertices) {
      LOG.info(jobVertex.toString());
    }
    for (JobEdge jobEdge : jobEdges) {
      LOG.info(jobEdge.toString());
    }
    for (IndependentOperatorDescriptor independentOperatorDescriptor :
        independentOperatorDescriptors) {
      LOG.info(independentOperatorDescriptor.toString());
    }
  }

  public boolean isCrossLanguageGraph() {
    Language language = jobVertices.get(0).getLanguage();
    for (JobVertex jobVertex : jobVertices) {
      if (jobVertex.getLanguage() != language) {
        return true;
      }
    }
    return false;
  }

  public List<JobEdge> getOutputEdgesByJobVertexId(int jobVertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getSourceVertexId() == jobVertexId)
        .collect(Collectors.toList());
  }

  public List<JobEdge> getInputEdgesByJobVertexId(int jobVertexId) {
    return jobEdges.stream()
        .filter(jobEdge -> jobEdge.getTargetVertexId() == jobVertexId)
        .collect(Collectors.toList());
  }

  public int markCyclicEdge() {
    jobVertices =
        jobVertices.stream()
            .sorted(
                Comparator.comparing(
                    jobVertex -> getInputEdgesByJobVertexId(jobVertex.getVertexId()).size()))
            .collect(Collectors.toList());

    int cyclicCount = 0;
    for (JobVertex vertex : jobVertices) {
      HashSet<JobVertex> visited = new HashSet<>();
      Stack<JobVertex> stack = new Stack<>();
      while (searchCyclicEdge(vertex, visited, stack)) {
        cyclicCount++;
        visited.clear();
        stack.clear();
      }
    }
    return cyclicCount;
  }

  private Boolean searchCyclicEdge(
      JobVertex curVertex, HashSet<JobVertex> visited, Stack<JobVertex> stack) {
    visited.add(curVertex);
    stack.push(curVertex);
    for (ListIterator<JobEdge> iterator =
            getOutputEdgesByJobVertexId(curVertex.getVertexId()).listIterator();
        iterator.hasNext(); ) {

      JobEdge edge = iterator.next();
      if (edge.isCyclic()) {
        continue;
      }
      JobVertex nextVertex = getVertex(edge.getTargetVertexId());
      if (!stack.contains(nextVertex)) {
        if (!visited.contains(nextVertex)) {
          if (searchCyclicEdge(nextVertex, visited, stack)) {
            return true;
          }
        }
      } else {
        edge.setCyclic(true);
        return true;
      }
    }
    stack.pop();
    return false;
  }

  @Override
  public JobGraph clone() {
    byte[] jobGraphBytes = Serializer.encode(this);
    return Serializer.decode(jobGraphBytes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobName", jobName)
        .add("jobConfig", jobConfig)
        .add("digraph", digraph)
        .toString();
  }
}
