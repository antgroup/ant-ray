package io.ray.streaming.operator.proxy;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

public class OperatorProxy implements StreamOperator {
  protected final StreamOperator operator;
  private int id;

  public OperatorProxy(StreamOperator operator) {
    this.operator = operator;
    this.id = operator.getId();
  }

  @Override
  public String getName() {
    return operator.getName();
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    operator.open(collectors, runtimeContext);
  }

  @Override
  public void saveCheckpoint(long checkpointId) throws Exception {
    operator.saveCheckpoint(checkpointId);
  }

  @Override
  public void loadCheckpoint(long checkpointId) throws Exception {
    operator.loadCheckpoint(checkpointId);
  }

  @Override
  public boolean isReadyRescaling() {
    return operator.isReadyRescaling();
  }

  @Override
  public void deleteCheckpoint(long checkpointId) throws Exception {
    operator.deleteCheckpoint(checkpointId);
  }

  @Override
  public void addNextOperator(StreamOperator operator) {
    this.operator.addNextOperator(operator);
  }

  @Override
  public void forwardCommand(String commandMessage) {}

  @Override
  public List<StreamOperator> getNextOperators() {
    return this.operator.getNextOperators();
  }

  @Override
  public void close() {
    operator.close();
  }

  @Override
  public void finish(long checkpointId) throws Exception {
    this.operator.finish(checkpointId);
  }

  @Override
  public Function getFunction() {
    return operator.getFunction();
  }

  @Override
  public Language getLanguage() {
    return operator.getLanguage();
  }

  @Override
  public Map<String, String> getOpConfig() {
    if (operator.getOpConfig() == null) {
      return new HashMap<>();
    }
    return operator.getOpConfig();
  }

  @Override
  public OperatorInputType getOpType() {
    return operator.getOpType();
  }

  @Override
  public ChainStrategy getChainStrategy() {
    return operator.getChainStrategy();
  }

  @Override
  public boolean hasTypeInfo() {
    return operator.hasTypeInfo();
  }

  @Override
  public TypeInfo getTypeInfo() {
    return operator.getTypeInfo();
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) {
    operator.setTypeInfo(typeInfo);
  }

  @Override
  public boolean hasSchema() {
    return operator.hasSchema();
  }

  @Override
  public Schema getSchema() {
    return operator.getSchema();
  }

  @Override
  public void setSchema(Schema schema) {
    operator.setSchema(schema);
  }

  public StreamOperator getOriginalOperator() {
    return operator;
  }

  @Override
  public void setResource(String resourceKey, Double resourceValue) {
    this.operator.setResource(resourceKey, resourceValue);
  }

  @Override
  public void setResource(Map<String, Double> resources) {
    this.operator.setResource(resources);
  }

  @Override
  public Map<String, Double> getResource() {
    return this.operator.getResource();
  }

  @Override
  public void closeState() {}

  @Override
  public boolean isRollback() {
    return false;
  }

  @Override
  public List<Collector> getCollectors() {
    return this.operator.getCollectors();
  }

  @Override
  public void process(Object record) {
    operator.process(record);
  }

  @Override
  public int getId() {
    return id;
  }
}
