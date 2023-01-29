package io.ray.streaming.operator.proxy;

import com.google.common.reflect.TypeToken;
import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.fury.row.binary.BinaryRow;
import io.ray.fury.util.MemoryUtils;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "UnstableApiUsage"})
public class OutputOperatorProxy extends OperatorProxy implements StreamOperator {
  private static final Logger LOG = LoggerFactory.getLogger(OutputOperatorProxy.class);

  private Encoder encoder;
  private TypeInfo typeInfo;
  private boolean disableOutputEncode;
  private int id;

  OutputOperatorProxy(StreamOperator operator) {
    super(operator);
    this.id = operator.getId();
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo != null ? typeInfo : operator.getTypeInfo();
  }

  public void disableOutputEncode(boolean disableOutputEncode) {
    this.disableOutputEncode = disableOutputEncode;
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    if (disableOutputEncode) {
      LOG.info("Output encode is disabled");
      operator.open(collectors, runtimeContext);
      return;
    }
    try {
      encoder = Encoders.bean(TypeToken.of(getTypeInfo().getType()).getRawType());
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Can't create encoder for type %s. Operator is %s, function is %s.",
              getTypeInfo(), operator, getFunction()),
          e);
    }
    LOG.info("Open operator with output encoder {} schema {}", encoder, encoder.schema());
    // Wrap all collectors to use encoder to serialize data.
    collectors =
        collectors.stream()
            .map(
                collector ->
                    new Collector<Record>() {
                      @Override
                      public void collect(Record value) {
                        value.setValue(encoder.toRow(value.getValue()).toBytes());
                        collector.collect(value);
                      }

                      @Override
                      public void retract(Record value) {
                        value.setValue(encoder.toRow(value.getValue()).toBytes());
                        collector.retract(value);
                      }
                    })
            .collect(Collectors.toList());
    operator.open(collectors, runtimeContext);
  }

  @Override
  public int getId() {
    return id;
  }

  protected void decodeValue(Encoder encoder, Record record) {
    if (encoder != null) {
      byte[] bytes = (byte[]) record.getValue();
      BinaryRow row = new BinaryRow(encoder.schema());
      row.pointTo(MemoryUtils.wrap(bytes), 0, bytes.length);
      Object value = encoder.fromRow(row);
      record.setValue(value);
    }
  }
}
