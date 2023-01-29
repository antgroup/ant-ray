package io.ray.streaming.jobgraph;

import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.ForwardPartition;
import io.ray.streaming.api.partition.impl.PythonPartitionFunction;
import io.ray.streaming.api.partition.impl.RoundRobinPartitionFunction;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.python.PythonJoinOperator;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonOperator.ChainedPythonOperator;
import io.ray.streaming.util.OperatorUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimize the job diagram so that operators can run in the same operator by linking operators of
 * the same partition and parallelism.
 */
public class JobGraphOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(JobGraphOptimizer.class);
  private final JobGraph jobGraph;
  private Set<JobVertex> visited = new HashSet<>();
  // vertex id -> vertex
  private Map<Integer, JobVertex> vertexMap;
  private Map<JobVertex, Set<JobEdge>> outputEdgesMap;
  // vertex id -> chained vertices
  private Map<Integer, List<JobVertex>> chainSet;
  // head vertex id -> the edges to another chainedOperator
  private Map<Integer, List<JobEdge>> tailEdgesMap;

  public JobGraphOptimizer(JobGraph jobGraph) {
    tailEdgesMap = new HashMap<>();
    chainSet = new HashMap<>();
    this.jobGraph = jobGraph;
    vertexMap =
        jobGraph.getJobVertices().stream()
            .collect(Collectors.toMap(JobVertex::getVertexId, Function.identity()));
    outputEdgesMap =
        vertexMap.keySet().stream()
            .collect(
                Collectors.toMap(
                    id -> vertexMap.get(id),
                    id -> new HashSet<>(jobGraph.getVertexOutputEdges(id))));
  }

  public JobGraph optimize() {
    // Deep-first traverse nodes from source to sink to merge vertices that can be chained
    // together.
    this.jobGraph
        .getJobVertices()
        .forEach(
            vertex -> {
              resetNextOperator(vertex.getOperator());
            });
    this.jobGraph
        .getSourceVertices()
        .forEach(
            vertex -> {
              addVertexToChainSet(vertex.getVertexId(), vertex);
              divideVertices(vertex.getVertexId(), vertex);
            });
    return new JobGraph(
        jobGraph.getJobName(),
        jobGraph.getJobConfig(),
        mergeVertex(),
        resetEdges(),
        this.jobGraph.getIndependentOperators());
  }

  void resetNextOperator(StreamOperator operator) {
    operator.setNextOperators(new ArrayList<>());
  }

  /**
   * Divide the original DAG into multiple chainedOperators.
   *
   * @param headVertexId The head vertex id of this chainedOperator.
   * @param srcVertex The source vertex of this edge.
   */
  public void divideVertices(int headVertexId, JobVertex srcVertex) {
    Set<JobEdge> outputEdges = outputEdgesMap.get(srcVertex);
    for (JobEdge outputEdge : outputEdges) {
      int targetId = outputEdge.getTargetVertexId();
      JobVertex targetVertex = vertexMap.get(targetId);
      srcVertex.getOperator().addNextOperator(targetVertex.getOperator());
      if (canChain(srcVertex, targetVertex, outputEdge)) {
        addVertexToChainSet(headVertexId, targetVertex);
        divideVertices(headVertexId, targetVertex);

      } else {
        addTailEdge(
            headVertexId,
            outputEdge,
            srcVertex.getOperator().getId(),
            outputEdge.getTargetOperatorId());
        if (visited.add(targetVertex)) {
          addVertexToChainSet(targetId, targetVertex);
          divideVertices(targetId, targetVertex);
        }
      }
    }
  }

  public List<JobVertex> mergeVertex() {
    List<JobVertex> newJobVertices = new ArrayList<>();
    chainSet.forEach(
        (headVertexId, treeList) -> {
          JobVertex mergedVertex;
          JobVertex headVertex = treeList.get(0);
          Language language = headVertex.getLanguage();

          if (treeList.size() == 1) {
            // no chain
            mergedVertex = headVertex;
          } else {
            List<StreamOperator> operators =
                treeList.stream()
                    .map(v -> vertexMap.get(v.getVertexId()).getOperator())
                    .collect(Collectors.toList());
            List<Map<String, String>> opConfigs =
                treeList.stream()
                    .map(v -> vertexMap.get(v.getVertexId()).getOperator().getOpConfig())
                    .collect(Collectors.toList());
            List<Map<String, Double>> resources =
                treeList.stream()
                    .map(v -> vertexMap.get(v.getVertexId()).getOperator().getResource())
                    .collect(Collectors.toList());

            AbstractStreamOperator operator;
            if (language == Language.JAVA) {
              operator = OperatorUtil.newChainedOperator(operators, opConfigs, resources);
            } else {
              List<PythonOperator> pythonOperators =
                  operators.stream()
                      .map(
                          op -> {
                            if (op instanceof PythonJoinOperator) {
                              return (PythonJoinOperator) op;
                            }
                            return (PythonOperator) op;
                          })
                      .collect(Collectors.toList());
              operator = new ChainedPythonOperator(pythonOperators, opConfigs, resources);
            }
            // chained operator config is placed into `ChainedOperator`.
            mergedVertex =
                new JobVertex(
                    headVertex.getVertexId(),
                    headVertex.getParallelism(),
                    headVertex.getDynamicDivisionNum(),
                    headVertex.getVertexType(),
                    operator);
          }
          newJobVertices.add(mergedVertex);
        });
    return newJobVertices;
  }

  /**
   * @param headVertexId The head vertex id of this chainedOperator.
   * @param outputEdge the edges to another chainedOperator.
   */
  private void addTailEdge(
      Integer headVertexId, JobEdge outputEdge, int srcOperatorId, int targetOperatorId) {
    JobEdge newJobEdge =
        new JobEdge(
            outputEdge.getSourceVertexId(),
            outputEdge.getTargetVertexId(),
            srcOperatorId,
            targetOperatorId,
            outputEdge.getPartition());
    newJobEdge.setEdgeType(outputEdge.getEdgeType());
    if (tailEdgesMap.containsKey(headVertexId)) {
      tailEdgesMap.get(headVertexId).add(newJobEdge);
    } else {
      List<JobEdge> outputEdges = new ArrayList<>();
      outputEdges.add(newJobEdge);
      tailEdgesMap.put(headVertexId, outputEdges);
    }
  }

  private List<JobEdge> resetEdges() {
    List<JobEdge> newJobEdges = new ArrayList<>();
    tailEdgesMap.forEach(
        (headVertexId, tailEdges) -> {
          tailEdges.forEach(
              tailEdge -> {
                // change ForwardPartition to RoundRobinPartition if necessary.
                Partition partition = changePartition(tailEdge.getPartition());
                JobEdge newEdge =
                    new JobEdge(
                        headVertexId,
                        tailEdge.getTargetVertexId(),
                        tailEdge.getSourceOperatorId(),
                        tailEdge.getTargetOperatorId(),
                        partition);
                newEdge.setEdgeType(tailEdge.getEdgeType());
                newJobEdges.add(newEdge);
              });
        });
    newJobEdges.sort(Comparator.comparingInt(JobEdge::getEdgeType));
    return newJobEdges;
  }

  private void addVertexToChainSet(Integer root, JobVertex targetVertex) {
    if (chainSet.containsKey(root)) {
      chainSet.get(root).add(targetVertex);
    } else {
      List<JobVertex> vertexList = new ArrayList<>();
      vertexList.add(targetVertex);
      chainSet.put(root, vertexList);
    }
  }

  private boolean canChain(JobVertex srcVertex, JobVertex targetVertex, JobEdge edge) {
    if (srcVertex.getParallelism() != targetVertex.getParallelism()) {
      return false;
    }
    if (srcVertex.getOperator().getChainStrategy() == ChainStrategy.NEVER
        || targetVertex.getOperator().getChainStrategy() == ChainStrategy.NEVER
        || targetVertex.getOperator().getChainStrategy() == ChainStrategy.HEAD) {
      return false;
    }
    int inputEdgesNum = jobGraph.getVertexInputEdges(targetVertex.getVertexId()).size();
    if (jobGraph.getVertexInputEdges(targetVertex.getVertexId()).size() > 1) {
      return false;
    }
    if (srcVertex.getLanguage() != targetVertex.getLanguage()) {
      return false;
    }
    Partition partition = edge.getPartition();
    if (!(partition instanceof PythonPartitionFunction)) {
      return partition instanceof ForwardPartition;
    } else {
      PythonPartitionFunction pythonPartition = (PythonPartitionFunction) partition;
      return !pythonPartition.isConstructedFromBinary()
          && pythonPartition
              .getFunctionName()
              .equals(PythonPartitionFunction.FORWARD_PARTITION_CLASS);
    }
  }

  /** Change ForwardPartition to RoundRobinPartition if necessary. */
  private Partition changePartition(Partition partition) {
    if (partition instanceof PythonPartitionFunction) {
      PythonPartitionFunction pythonPartition = (PythonPartitionFunction) partition;
      if (!pythonPartition.isConstructedFromBinary()
          && pythonPartition
              .getFunctionName()
              .equals(PythonPartitionFunction.FORWARD_PARTITION_CLASS)) {
        return PythonPartitionFunction.RoundRobinPartition;
      } else {
        return partition;
      }
    } else {
      if (partition instanceof ForwardPartition) {
        return new RoundRobinPartitionFunction();
      } else {
        return partition;
      }
    }
  }
}
