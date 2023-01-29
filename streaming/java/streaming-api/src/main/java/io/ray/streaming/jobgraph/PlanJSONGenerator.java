package io.ray.streaming.jobgraph;

import com.google.gson.Gson;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.chain.ChainedOperator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanJSONGenerator {

  private JobGraph plan;
  private JsonPlan jsonPlan;

  public PlanJSONGenerator(JobGraph plan) {
    this.plan = plan;
    this.jsonPlan = new JsonPlan();
  }

  public JsonPlan getJsonPlan() {
    List<JobVertex> vertexSet = new ArrayList<>(plan.getJobVertices());
    // 序号大的只能依赖序号小的
    vertexSet.sort(Comparator.comparingInt(JobVertex::getVertexId));

    visit(vertexSet);
    return jsonPlan;
  }

  public String getJsonString() {
    Gson gson = new Gson();
    return gson.toJson(getJsonPlan());
  }

  private void visit(List<JobVertex> toVisit) {

    JobVertex vertex = toVisit.get(0);
    Set<JobEdge> inputEdges = plan.getVertexInputEdges(vertex.getVertexId());
    Set<Integer> srcVertexSet = new HashSet<>();
    for (JobEdge edge : inputEdges) {
      srcVertexSet.add(edge.getSourceVertexId());
    }

    Vertex v = new Vertex();
    decorateNode(vertex, v, vertex.getOperator().getTailOperatorSet());
    jsonPlan.vertices.put(v.id, v);
    toVisit.remove(vertex);

    if (!toVisit.isEmpty()) {
      visit(toVisit);
    }
  }

  /**
   * 节点属性装饰函数.
   *
   * @param jobVertex 逻辑或者物理计划节点
   * @param vertex json展示节点
   */
  private void decorateNode(JobVertex jobVertex, Vertex vertex, Set<StreamOperator> tail) {
    vertex.id = Integer.toString(jobVertex.getVertexId());
    vertex.vertexType = jobVertex.getVertexType();
    vertex.parallelism = jobVertex.getParallelism();
    vertex.vertexMode = jobVertex.getVertexMode();

    if (vertex.vertexType != VertexType.source) {
      for (JobEdge edge : plan.getVertexInputEdges(jobVertex.getVertexId())) {
        Predecessor predecessor = new Predecessor();
        predecessor.id = Integer.toString(edge.getSourceVertexId());
        predecessor.partitionType = edge.getPartition().getPartitionType();
        vertex.parents.add(predecessor);
      }
    }

    if (jobVertex.getOperator().getNextOperators().size() > 0) {
      vertex.innerPlan = new JsonPlan();
      if (jobVertex.getOperator() instanceof ChainedOperator) {
        decorateInnerOperator(
            vertex.innerPlan,
            jobVertex.getOperator().getNextOperators().get(0),
            vertex,
            null,
            tail);
      } else {
        vertex.operator = jobVertex.getOperator().getOperatorString();
        vertex.operatorName = jobVertex.getOperator().getName();
        vertex.function = jobVertex.getOperator().getFunctionString();
      }
    } else {
      vertex.operator = jobVertex.getOperator().getOperatorString();
      vertex.operatorName = jobVertex.getOperator().getName();
      vertex.function = jobVertex.getOperator().getFunctionString();
    }
  }

  /** 物理节点内部DAG属性装饰函数. */
  private void decorateInnerOperator(
      JsonPlan innerPlan,
      StreamOperator operator,
      Vertex outerVertex,
      String parentId,
      Set<StreamOperator> tail) {
    Vertex vertex = new Vertex();
    vertex.operator = operator.getOperatorString();
    vertex.id = outerVertex.id + "-" + operator.getId();
    vertex.operatorName = operator.getName();
    vertex.function = operator.getFunctionString();
    vertex.parallelism = outerVertex.parallelism;
    innerPlan.vertices.put(vertex.id, vertex);
    if (parentId != null) {
      Predecessor predecessor = new Predecessor();
      predecessor.id = parentId;
      vertex.parents.add(predecessor);
    }
    if (tail.contains(operator)) {
      return;
    }

    for (StreamOperator op : operator.getNextOperators()) {
      if ("".equals(op.getName())) {
        continue;
      }
      decorateInnerOperator(innerPlan, op, outerVertex, vertex.id, tail);
    }
  }

  public static class JsonPlan {

    public Map<String, Vertex> vertices = new HashMap<>();
  }

  public static class Predecessor {
    public String id;
    public Partition.PartitionType partitionType;
  }

  public static class Vertex {

    public VertexType vertexType;
    public VertexMode vertexMode;
    public String id;
    public int parallelism;
    public String operator;
    public String operatorName;
    public String function;
    public List<Predecessor> parents = new ArrayList<>();
    public JsonPlan innerPlan;
  }
}
