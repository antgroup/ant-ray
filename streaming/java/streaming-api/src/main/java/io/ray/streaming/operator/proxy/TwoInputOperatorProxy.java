package io.ray.streaming.operator.proxy;

import com.google.common.reflect.TypeToken;
import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.TwoInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "UnstableApiUsage"})
public class TwoInputOperatorProxy extends OutputOperatorProxy implements TwoInputOperator {
  private static final Logger LOG = LoggerFactory.getLogger(TwoInputOperatorProxy.class);

  private TypeInfo leftInputTypeInfo;
  private TypeInfo rightInputTypeInfo;
  private Encoder leftEncoder;
  private Encoder rightEncoder;
  private boolean disableInputDecode;

  public TwoInputOperatorProxy(TwoInputOperator operator) {
    super(operator);
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);
    if (!disableInputDecode) {
      try {
        if (getLeftInputTypeInfo() != null) {
          leftEncoder = Encoders.bean(TypeToken.of(getLeftInputTypeInfo().getType()).getRawType());
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format(
                "Can't create leftDecoder for type %s. Operator is %s, function is %s.",
                getLeftInputTypeInfo(), operator, getFunction()),
            e);
      }
      try {
        if (getRightInputTypeInfo() != null) {
          rightEncoder =
              Encoders.bean(TypeToken.of(getRightInputTypeInfo().getType()).getRawType());
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(
            String.format(
                "Can't create rightDecoder for type %s. Operator is %s, function is %s.",
                getRightInputTypeInfo(), operator, getFunction()),
            e);
      }
      LOG.info(
          "Open operator proxy with left typeinfo {} left encoder {} and right typeinfo {} "
              + "right encoder {}",
          getLeftInputTypeInfo(),
          leftEncoder,
          getRightInputTypeInfo(),
          rightEncoder);
    }
  }

  @Override
  public void processElement(Record record1, Record record2) throws Exception {
    if (record1 != null) {
      decodeValue(leftEncoder, record1);
    }
    if (record2 != null) {
      decodeValue(rightEncoder, record2);
    }
    ((TwoInputOperator) operator).processElement(record1, record2);
  }

  public void setLeftInputTypeInfo(TypeInfo leftInputTypeInfo) {
    this.leftInputTypeInfo = leftInputTypeInfo;
  }

  public void setRightInputTypeInfo(TypeInfo rightInputTypeInfo) {
    this.rightInputTypeInfo = rightInputTypeInfo;
  }

  @Override
  public TypeInfo getLeftInputTypeInfo() {
    return leftInputTypeInfo != null
        ? leftInputTypeInfo
        : ((TwoInputOperator) operator).getLeftInputTypeInfo();
  }

  @Override
  public TypeInfo getRightInputTypeInfo() {
    return rightInputTypeInfo != null
        ? rightInputTypeInfo
        : ((TwoInputOperator) operator).getRightInputTypeInfo();
  }

  public void disableInputDecode(boolean disableInputDecode) {
    this.disableInputDecode = disableInputDecode;
  }
}
