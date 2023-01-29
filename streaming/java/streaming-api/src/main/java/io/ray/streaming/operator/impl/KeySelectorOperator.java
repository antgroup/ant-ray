package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.util.TypeInfo;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySelectorOperator<I, K> extends AbstractStreamOperator<KeyFunction<I, K>>
    implements OneInputOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeySelectorOperator.class);

  public KeySelectorOperator() {}

  public KeySelectorOperator(KeyFunction<I, K> keySelector) {
    super(keySelector);
  }

  @Override
  public void processElement(Record record) throws Exception {
    K key = null;
    try {
      key = function.keyBy((I) record.getValue());
    } catch (Exception e) {
      LOGGER.error(
          "Key selector failed, value : {}, cause : {}", record.getValue(), e.getMessage());
      throw new RuntimeException(e);
    }
    if (key == null) {
      key = (K) new Null();
    }
    collect(key, record.getValue());
  }

  @Override
  public OperatorInputType getOpType() {
    return OperatorInputType.ONE_INPUT;
  }

  @Override
  public void process(Object record) {}

  @Override
  public TypeInfo getInputTypeInfo() {
    return null;
  }

  static class Null implements Serializable {

    @Override
    public int hashCode() {
      return 1;
    }

    @SuppressWarnings("EqualsHashCode")
    @Override
    public boolean equals(Object obj) {
      return obj instanceof Null;
    }
  }
}
