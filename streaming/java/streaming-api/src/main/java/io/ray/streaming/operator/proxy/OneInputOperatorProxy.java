package io.ray.streaming.operator.proxy;

import com.google.common.reflect.TypeToken;
import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "UnstableApiUsage"})
public class OneInputOperatorProxy extends OutputOperatorProxy implements OneInputOperator {
  private static final Logger LOG = LoggerFactory.getLogger(OneInputOperatorProxy.class);

  private TypeInfo inputTypeInfo;
  private Encoder encoder;
  private boolean disableInputDecode;

  public OneInputOperatorProxy(OneInputOperator operator) {
    super(operator);
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    super.open(collectors, runtimeContext);
    if (!disableInputDecode && getInputTypeInfo() != null) {
      try {
        encoder = Encoders.bean(TypeToken.of(getInputTypeInfo().getType()).getRawType());
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Can't create decoder for type %s. Operator is %s, function is %s.",
                getInputTypeInfo(), operator, getFunction()),
            e);
      }
      LOG.info(
          "Open operator proxy with typeinfo {} encoder {} and schema {}",
          getInputTypeInfo(),
          encoder,
          encoder);
    }
  }

  @Override
  public void processElement(Record record) throws Exception {
    if (encoder != null) {
      decodeValue(encoder, record);
    }
    ((OneInputOperator) operator).processElement(record);
  }

  public void setInputTypeInfo(TypeInfo inputTypeInfo) {
    this.inputTypeInfo = inputTypeInfo;
  }

  @Override
  public TypeInfo getInputTypeInfo() {
    return inputTypeInfo != null ? inputTypeInfo : ((OneInputOperator) operator).getInputTypeInfo();
  }

  public void disableInputDecode(boolean disableInputDecode) {
    this.disableInputDecode = disableInputDecode;
  }
}
