package io.ray.streaming.operator.impl;

import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.fury.util.MemoryBuffer;
import io.ray.fury.util.MemoryUtils;
import io.ray.fury.vectorized.ArrowUtils;
import io.ray.fury.vectorized.ArrowWriter;
import io.ray.streaming.DefaultRuntimeContext;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.impl.ArrowOperator.ArrowRecordBatchWithSchema;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrowOperatorTest {

  public static class A {
    String f1;
    int f2;
  }

  private MapOperator<String, A> inputOperator =
      new MapOperator<>(
          new MapFunction<String, A>() {
            @Override
            public A map(String value) {
              return null;
            }
          });

  @Test
  public void testProcessElement() {
    ArrowOperator<Object> arrowOperator = new ArrowOperator<>(inputOperator, 10, 10000000);
    check(arrowOperator, 10);
  }

  @Test(timeOut = 10_000)
  public void testTimeout() {
    ArrowOperator<Object> arrowOperator = new ArrowOperator<>(inputOperator, 10000000, 500);
    check(arrowOperator, 10);
  }

  private void check(ArrowOperator<Object> arrowOperator, int batchSize) {
    int numBatches = 3;
    Queue<ArrowRecordBatch> out = new LinkedList<>();
    Collector<Record> collector =
        new Collector<Record>() {

          @Override
          public void collect(Record value) {
            out.add((ArrowRecordBatch) value.getValue());
          }

          @Override
          public void retract(Record value) {
            collect(value);
          }
        };
    Encoder<A> encoder = Encoders.bean(A.class);
    ArrowWriter arrowWriter = ArrowUtils.createArrowWriter(encoder.schema());
    A a = new A();
    a.f1 = "str";
    a.f2 = 10;
    for (int i = 0; i < batchSize; i++) {
      arrowWriter.write(encoder.toRow(a));
    }
    ArrowRecordBatch expectedRecordBatch = arrowWriter.finishAsRecordBatch();
    List<Collector> collectors = Collections.singletonList(collector);
    arrowOperator.open(collectors, new DefaultRuntimeContext());
    for (int i = 0; i < numBatches; i++) {
      System.out.printf("numBatch: %s\n", i);
      for (int j = 0; j < batchSize; j++) {
        arrowOperator.processElement(new Record<>(a));
      }
      while (out.size() == 0) {
        try {
          TimeUnit.MILLISECONDS.sleep(arrowOperator.getTimeoutMilliseconds());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      ArrowRecordBatchWithSchema recordBatch = (ArrowRecordBatchWithSchema) out.remove();
      Assert.assertEquals(recordBatch.getLength(), expectedRecordBatch.getLength());
      Assert.assertEquals(recordBatch.getBuffersLayout(), expectedRecordBatch.getBuffersLayout());
      MemoryBuffer buffer1 = MemoryUtils.buffer(32);
      MemoryBuffer buffer2 = MemoryUtils.buffer(32);
      ArrowUtils.serializeRecordBatch(expectedRecordBatch, buffer1);
      ArrowUtils.serializeRecordBatch(recordBatch, buffer2);
      Assert.assertTrue(
          buffer1.equalTo(buffer2, 0, 0, Math.max(buffer1.writerIndex(), buffer2.writerIndex())));
      Assert.assertEquals(recordBatch.getSchema(), encoder.schema());
      // Indicate that we have finished consuming this batch
      recordBatch.close();
      Assert.assertTrue(recordBatch.isClosed());
    }
    arrowWriter.reset();
  }
}
