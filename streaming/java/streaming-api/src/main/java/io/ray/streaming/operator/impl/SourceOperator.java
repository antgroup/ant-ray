package io.ray.streaming.operator.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.ISourceOperator;
import io.ray.streaming.util.EndOfDataException;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;

public class SourceOperator<T> extends AbstractStreamOperator<SourceFunction<T>>
    implements ISourceOperator {
  private SourceContextImpl sourceContext;

  public SourceOperator(SourceFunction<T> function) {
    super(function);
    setChainStrategy(ChainStrategy.HEAD);
    Method method = TypeUtils.getFunctionMethod(SourceFunction.class, function, "fetch");
    Type sourceCtxType = method.getGenericParameterTypes()[1];
    Type sourceCtxGenericType = TypeUtils.getSam(SourceContext.class).getGenericParameterTypes()[0];
    Type type = TypeUtils.resolveType(sourceCtxType, sourceCtxGenericType);
    typeInfo = new TypeInfo(type);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    this.sourceContext = new SourceContextImpl(collectorList);
    this.function.init(runtimeContext.getTaskParallelism(), runtimeContext.getTaskIndex());
  }

  @Override
  public void fetch(long checkpointId) {
    try {
      this.sourceContext.setCheckpointId(checkpointId);
      this.function.fetch(checkpointId, this.sourceContext);
    } catch (EndOfDataException e) {
      throw new EndOfDataException();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SourceContext getSourceContext() {
    return sourceContext;
  }

  @Override
  public OperatorInputType getOpType() {
    return OperatorInputType.SOURCE;
  }

  class SourceContextImpl implements SourceContext<T> {
    private List<Collector> collectors;
    private long checkpointId;

    public SourceContextImpl(List<Collector> collectors) {
      this.collectors = collectors;
    }

    public void setCheckpointId(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    @Override
    public void collect(T t) throws Exception {
      for (Collector collector : collectors) {
        collector.collect(new Record<>(t));
      }
    }
  }
}
