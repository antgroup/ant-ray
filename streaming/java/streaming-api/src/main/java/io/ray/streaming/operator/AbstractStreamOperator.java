package io.ray.streaming.operator;

import com.google.common.base.Preconditions;
import io.ray.fury.types.TypeInference;
import io.ray.fury.util.Tuple2;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.RichFunction;
import io.ray.streaming.api.function.internal.Functions;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStreamOperator<F extends Function> implements StreamOperator {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

  protected String name;

  int id;
  protected F function;
  protected RichFunction richFunction;
  protected List<Collector> collectorList;
  protected RuntimeContext runtimeContext;
  private ChainStrategy chainStrategy = ChainStrategy.ALWAYS;
  protected TypeInfo typeInfo;
  protected Schema schema;
  protected Map<String, Double> resources = new HashMap<>(4);
  protected Map<String, String> opConfig = new HashMap(4);
  protected List<StreamOperator> nextOperators = new ArrayList<>();

  protected AbstractStreamOperator() {
    this.name = getClass().getSimpleName();
  }

  protected AbstractStreamOperator(F function) {
    this();
    setFunction(function);
  }

  public void setFunction(F function) {
    this.function = function;
    this.richFunction = Functions.wrap(function);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    this.collectorList = collectorList;
    this.runtimeContext = runtimeContext;
    if (runtimeContext != null && runtimeContext.getOpConfig() != null) {
      runtimeContext.getOpConfig().putAll(getOpConfig());
    }

    richFunction.open(runtimeContext);
  }

  @Override
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean isReadyRescaling() {
    return true;
  }

  @Override
  public void process(Object record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveCheckpoint(long checkpointId) throws Exception {
    this.function.saveCheckpoint(checkpointId);
  }

  @Override
  public void deleteCheckpoint(long checkpointId) throws Exception {
    this.function.deleteCheckpoint(checkpointId);
  }

  @Override
  public void loadCheckpoint(long checkpointId) throws Exception {
    this.function.loadCheckpoint(checkpointId);
  }

  @Override
  public boolean isRollback() {
    return false;
  }

  @Override
  public void closeState() {}

  @Override
  public void close() {
    richFunction.close();
  }

  @Override
  public void finish(long checkpointId) throws Exception {
    this.function.finish(checkpointId);
  }

  @Override
  public Function getFunction() {
    return function;
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }

  protected <T> void collect(T record) {
    for (Collector collector : this.collectorList) {
      collector.collect(record);
    }
  }

  protected final <K, V> void collect(K key, V value) {
    if (value == null || key == null) {
      LOG.debug("collect null, key: {}, value: {}", key, value);
      return;
    }
    for (Collector collector : this.collectorList) {
      collector.collect(new KeyRecord(key, value));
    }
  }

  protected <T> void retract(T record) {
    for (Collector collector : this.collectorList) {
      collector.retract(record);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  public void setChainStrategy(ChainStrategy chainStrategy) {
    this.chainStrategy = chainStrategy;
  }

  @Override
  public ChainStrategy getChainStrategy() {
    return chainStrategy;
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  @Override
  public boolean hasTypeInfo() {
    return typeInfo != null;
  }

  @Override
  public TypeInfo getTypeInfo() {
    Preconditions.checkNotNull(typeInfo);
    return typeInfo;
  }

  @Override
  public boolean hasSchema() {
    return schema != null || computeSchema().isPresent();
  }

  @Override
  public Schema getSchema() {
    if (schema == null) {
      computeSchema().ifPresent(this::setSchema);
    }
    Preconditions.checkNotNull(schema);
    return schema;
  }

  private Optional<Schema> computeSchema() {
    if (hasTypeInfo() && getTypeInfo() != null && TypeInference.isBean(getTypeInfo().getType())) {
      Tuple2<Schema, String> either = TypeUtils.tryInferSchema(getTypeInfo().getType());
      if (either.f1 != null) {
        LOG.info("Can't infer schema for data type {}: {}", getTypeInfo(), either.f1);
      } else {
        return Optional.of(either.f0);
      }
    }
    return Optional.empty();
  }

  @Override
  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void setResource(String resourceKey, Double resourceValue) {
    this.resources.put(resourceKey, resourceValue);
  }

  @Override
  public void setResource(Map<String, Double> resources) {
    this.resources.putAll(resources);
  }

  @Override
  public Map<String, Double> getResource() {
    return this.resources;
  }

  @Override
  public List<Collector> getCollectors() {
    return this.collectorList;
  }

  @Override
  public Map<String, String> getOpConfig() {
    if (opConfig == null) {
      return new HashMap<>();
    }
    return opConfig;
  }

  @Override
  public String getFunctionString() {
    if (function != null) {
      if (function.getClass().getSimpleName().length() == 0) {
        return "Anonymous";
      }
      return function.getClass().getSimpleName();
    }
    return "";
  }

  @Override
  public List<StreamOperator> getNextOperators() {
    return nextOperators;
  }

  @Override
  public void addNextOperator(StreamOperator operator) {
    nextOperators.add(operator);
  }

  @Override
  public void setNextOperators(List<StreamOperator> operatorList) {
    nextOperators = operatorList;
  }

  @Override
  public void forwardCommand(String commandMessage) {
    if (function == null) {
      throw new RuntimeException(
          String.format("Function isn't initialized for forward-command: %s.", commandMessage));
    }
    function.forwardCommand(commandMessage);
  }
}
