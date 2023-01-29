package io.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.common.serializer.Serializer;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.chain.ChainedOperator;
import io.ray.streaming.python.PythonOperator;
import java.io.Serializable;
import java.util.Map;

/** Job vertex is a cell node where logic is executed. */
public class JobVertex implements Serializable {

  private final int vertexId;

  private final int dynamicDivisionNum;
  private final VertexType vertexType;

  private VertexMode vertexMode = VertexMode.update;
  private Language language;
  private StreamOperator operator;
  private int parallelism;

  public JobVertex(
      int vertexId,
      int parallelism,
      int dynamicDivisionNum,
      VertexType vertexType,
      AbstractStreamOperator<?> operator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.dynamicDivisionNum = dynamicDivisionNum;
    this.vertexType = vertexType;
    this.operator = operator;
    this.language = operator.getLanguage();
    operator.setId(vertexId);
  }

  public int getVertexId() {
    return vertexId;
  }

  public String getIdStr() {
    return String.valueOf(vertexId);
  }

  public int getDynamicDivisionNum() {
    return dynamicDivisionNum;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public VertexMode getVertexMode() {
    return vertexMode;
  }

  public void setVertexMode(VertexMode vertexMode) {
    this.vertexMode = vertexMode;
  }

  public StreamOperator getOperator() {
    return operator;
  }

  public void setOperator(StreamOperator operator) {
    this.operator = operator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public String getVertexName() {
    String opName = this.operator == null ? "default-operator" : this.operator.getName();
    return this.vertexId + "-" + opName;
  }

  public String getVertexString() {
    String operatorStr = operator.toString();
    operatorStr = operatorStr.substring(0, operatorStr.length() - 1);
    operatorStr = operatorStr.replaceAll("\\n", "\\\\n");

    return String.format("%s, p:%d, %s", getVertexName(), parallelism, operatorStr);
  }

  public Map<String, Double> getResources() {
    return operator.getResource();
  }

  public Map<String, Double> cloneResources() {
    byte[] encoded = Serializer.encode(getResources());
    return Serializer.decode(encoded);
  }

  public void setResources(Map<String, Double> resources) {
    operator.getResource().putAll(resources);
  }

  public String getOperatorNameWithIndex() {
    return getName();
  }

  public String getName() {
    if (this.operator != null) {
      if (operator instanceof ChainedOperator || operator instanceof PythonOperator) {
        return String.format("%s-%s", vertexId, operator.getName().split("\n")[0]).trim();
      }
      return String.format("%s-%s", vertexId, operator.getClass().getSimpleName().split("\n")[0])
          .trim();
    }
    return String.format("%s-%s", vertexId, StreamOperator.DEFAULT_NAME).trim();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexType", vertexType)
        .add("language", language)
        .add("operator", getOperatorNameWithIndex())
        .toString();
  }
}
