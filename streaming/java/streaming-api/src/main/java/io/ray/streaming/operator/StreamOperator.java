package io.ray.streaming.operator;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.common.generated.Common;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.util.TypeInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Schema;

public interface StreamOperator extends Serializable {

  String DEFAULT_NAME = "DefaultOperator";

  /** Get current operator's id. */
  int getId();

  /** Get current operator's name. */
  String getName();

  /** Get current operator's type. */
  OperatorInputType getOpType();

  /** Get current operator's chain strategy. */
  ChainStrategy getChainStrategy();

  /** Get current operator's function. */
  Function getFunction();

  /** Get current operator's language type. */
  Language getLanguage();

  /** Get current operator's config. */
  Map<String, String> getOpConfig();

  boolean isReadyRescaling();

  /** Get current operator's down stream collectors. */
  List<Collector> getCollectors();

  /** Open operator. */
  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  /**
   * Process record.
   *
   * @param record target object
   */
  void process(Object record);

  /** Close Operator. */
  void close();

  /** Finish this checkpoint in specific id. */
  void finish(long checkpointId) throws Exception;

  /** Save checkpoint by checkpoint id. */
  void saveCheckpoint(long checkpointId) throws Exception;

  /** Load checkpoint by checkpoint id. */
  void loadCheckpoint(long checkpointId) throws Exception;

  /** Delete checkpoint by checkpoint id. */
  default void deleteCheckpoint(long checkpointId) throws Exception {
    throw new UnsupportedOperationException();
  }

  /** Returns true if this operator has output data type information. */
  boolean hasTypeInfo();

  /** Returns Output data type information of this operator. */
  TypeInfo getTypeInfo();

  /** Set typeinfo for this operator. */
  void setTypeInfo(TypeInfo typeInfo);

  /** Returns true if this operator has output data schema. */
  boolean hasSchema();

  /** Returns Output data schema of this operator. */
  Schema getSchema();

  /** Set schema for this operator. */
  void setSchema(Schema schema);

  /** Set resource for operator. */
  void setResource(String resourceKey, Double resourceValue);

  /** Set resources for operator. */
  void setResource(Map<String, Double> resources);

  /** Get operator's resources. */
  Map<String, Double> getResource();

  default void closeState() {
    throw new UnsupportedOperationException();
  }

  default String getOperatorString() {
    return getClass().getSimpleName();
  }

  default boolean isRollback() {
    throw new UnsupportedOperationException();
  }

  default List<StreamOperator> getNextOperators() {
    return new ArrayList<>();
  }

  default Set<StreamOperator> getTailOperatorSet() {
    return new HashSet<>();
  }

  default void setNextOperators(List<StreamOperator> operators) {}

  default String getFunctionString() {
    return "";
  }

  void addNextOperator(StreamOperator operator);

  /* United Distributed Controller start. */
  default boolean onPrepare(Common.UnitedDistributedControlMessage controlMessage) {
    return getFunction().onPrepare(controlMessage);
  }

  default boolean onDisposed() {
    return getFunction().onDisposed();
  }

  default boolean onCancel() {
    return getFunction().onCancel();
  }

  default Common.UnitedDistributedControlMessage onBlockingCommit() {
    return getFunction().onBlockingCommit();
  }

  default void onNonBlockingCommit() {
    getFunction().onNonBlockingCommit();
  }
  /* United Distributed Controller end. */

  void forwardCommand(String commandMessage);
}
