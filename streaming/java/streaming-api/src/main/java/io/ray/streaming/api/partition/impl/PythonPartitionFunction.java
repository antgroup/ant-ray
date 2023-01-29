package io.ray.streaming.api.partition.impl;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.partition.Partition;
import java.util.StringJoiner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a python partition function.
 *
 * <p>Python worker can create a partition object using information in this PythonPartition.
 *
 * <p>If this object is constructed from serialized python partition, python worker can deserialize
 * it to create python partition directly. If this object is constructed from moduleName and
 * className/functionName, python worker will use `importlib` to load python partition function.
 */
public class PythonPartitionFunction implements Partition<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(PythonPartitionFunction.class);
  public static final PythonPartitionFunction BroadcastPartition =
      new PythonPartitionFunction("raystreaming.partition", "BroadcastPartition");
  public static final PythonPartitionFunction KeyPartition =
      new PythonPartitionFunction("raystreaming.partition", "KeyPartition");
  public static final PythonPartitionFunction RoundRobinPartition =
      new PythonPartitionFunction("raystreaming.partition", "RoundRobinPartition");
  public static final String FORWARD_PARTITION_CLASS = "ForwardPartition";
  public static final PythonPartitionFunction ForwardPartition =
      new PythonPartitionFunction("raystreaming.partition", FORWARD_PARTITION_CLASS);

  private byte[] partition;
  private String moduleName;
  private String functionName;

  public PythonPartitionFunction(byte[] partition) {
    Preconditions.checkNotNull(partition);
    this.partition = partition;
  }

  /**
   * Create a python partition from a moduleName and partition function name.
   *
   * @param moduleName module name of python partition.
   * @param functionName function/class name of the partition function.
   */
  public PythonPartitionFunction(String moduleName, String functionName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(moduleName));
    Preconditions.checkArgument(StringUtils.isNotBlank(functionName));
    this.moduleName = moduleName;
    this.functionName = functionName;
  }

  @Override
  public int[] partition(Object record, int currentIndex, int numPartition) {
    String msg =
        String.format("partition method of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public int[] partition(Object record, int numPartition) {
    String msg =
        String.format("partition method of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public PartitionType getPartitionType() {
    if (StringUtils.isNotBlank(functionName) && functionName.equals(KeyPartition.functionName)) {
      return PartitionType.key;
    } else {
      if (StringUtils.isEmpty(functionName)) {
        LOG.warn("function name is empty");
      }
      return PartitionType.forward;
    }
  }

  public byte[] getPartition() {
    return partition;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getFunctionName() {
    return functionName;
  }

  public boolean isConstructedFromBinary() {
    return partition != null;
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner =
        new StringJoiner(", ", PythonPartitionFunction.class.getSimpleName() + "[", "]");
    if (partition != null) {
      stringJoiner.add("partition=binary");
    } else {
      stringJoiner
          .add("moduleName='" + moduleName + "'")
          .add("functionName='" + functionName + "'");
    }
    return stringJoiner.toString();
  }
}
