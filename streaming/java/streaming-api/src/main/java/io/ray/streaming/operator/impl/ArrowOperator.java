package io.ray.streaming.operator.impl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.ray.fury.encoder.Encoder;
import io.ray.fury.encoder.Encoders;
import io.ray.fury.row.binary.BinaryRow;
import io.ray.fury.types.TypeInference;
import io.ray.fury.vectorized.ArrowUtils;
import io.ray.fury.vectorized.ArrowWriter;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.internal.Functions;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class ArrowOperator<T> extends AbstractStreamOperator<Function>
    implements OneInputOperator<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ArrowOperator.class);

  private final StreamOperator inputOperator;
  private transient Thread thread;
  private final int batchSize;
  private final long timeoutMilliseconds;
  private ReentrantLock lock;
  private transient Encoder<T> encoder;
  private transient ArrowWriter arrowWriter;
  private transient ArrowRecordBatchWithSchema lastRecordBatch;
  private long lastWriteTimeNs;
  private int rowCount;
  private List<BinaryRow> rowBuffer;

  public ArrowOperator(StreamOperator inputOperator, int batchSize, long timeoutMilliseconds) {
    super(Functions.emptyFunction());
    Preconditions.checkArgument(batchSize > 0);
    Preconditions.checkArgument(timeoutMilliseconds > 0);
    Preconditions.checkArgument(
        TypeInference.isBean(inputOperator.getTypeInfo().getType()),
        "Arrow record batch is only supported on bean classes.");
    LOG.info(
        "Create ArrowOperator with batchSize {} timeoutMilliseconds {}",
        batchSize,
        timeoutMilliseconds);
    this.inputOperator = inputOperator;
    this.batchSize = batchSize;
    this.timeoutMilliseconds = timeoutMilliseconds;
    rowBuffer = new ArrayList<>();
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    encoder = Encoders.bean((Class<T>) (inputOperator.getTypeInfo().getType()));
    arrowWriter = ArrowUtils.createArrowWriter(encoder.schema());
    LOG.info(
        "Create encoder {} and arrow writer {} with schema {}.",
        encoder,
        arrowWriter,
        encoder.schema());
    lock = new ReentrantLock();
    Runnable task =
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            LOG.debug("try collect an arrow record batch");
            lock.lock();
            try {
              if (lastRecordBatch == null || lastRecordBatch.isClosed()) {
                if (System.nanoTime() - lastWriteTimeNs >= timeoutMilliseconds * 1000_000L
                    && rowBuffer.size() > 0) {
                  LOG.info(
                      "Write {} pieces of data as arrow record batch after {} milliseconds "
                          + "timeouts",
                      rowBuffer.size(),
                      (System.nanoTime() - lastWriteTimeNs) / 1000_000L);
                  rowBuffer.subList(rowCount, rowBuffer.size()).forEach(arrowWriter::write);
                  ArrowRecordBatch batch = arrowWriter.finishAsRecordBatch();
                  lastRecordBatch = new ArrowRecordBatchWithSchema(encoder.schema(), batch);
                  batch.close();
                  Record newRecord = new Record(lastRecordBatch);
                  collect(newRecord);
                  rowCount = 0;
                  rowBuffer.clear();
                  lastWriteTimeNs = System.nanoTime();
                }
              }
            } finally {
              lock.unlock();
            }
            long sleepMills = timeoutMilliseconds - (System.nanoTime() - lastWriteTimeNs) / 1000;
            if (sleepMills <= 0) {
              sleepMills = timeoutMilliseconds / 10;
            }
            try {
              TimeUnit.MILLISECONDS.sleep(sleepMills);
            } catch (InterruptedException e) {
              LOG.info(
                  "{} thread sleep {}ms interrupt, exit this thread",
                  sleepMills,
                  Thread.currentThread());
              break;
            }
          }
        };
    thread =
        new ThreadFactoryBuilder()
            .setNameFormat("arrow-operator-flusher-%d")
            .setDaemon(true)
            .build()
            .newThread(task);
    thread.start();
  }

  @Override
  public void processElement(Record<T> record) {
    Preconditions.checkArgument(record.getClass() == Record.class);
    lock.lock();
    try {
      BinaryRow row = encoder.toRow(record.getValue());
      rowBuffer.add(row);
      // rowCount can be greater than 0 only if lastRecordBatch is null or lastRecordBatch has been
      // closed.
      if (lastRecordBatch == null) {
        rowCount++;
        arrowWriter.write(row);
      } else if (lastRecordBatch.isClosed()) {
        if (rowCount != 0) { // arrowWriter already reset
          rowCount++;
          arrowWriter.write(row);
        } else {
          // After reset, lastRecordBatch will be invalid.
          // So we need the user to close RecordBatch,so we can take it as a signal that we can
          // invalidate last RecordBatch.
          arrowWriter.reset();
          rowBuffer.forEach(arrowWriter::write);
          rowCount = rowBuffer.size();
        }
      }
      if (rowCount == batchSize) {
        ArrowRecordBatch batch = arrowWriter.finishAsRecordBatch();
        lastRecordBatch = new ArrowRecordBatchWithSchema(encoder.schema(), batch);
        batch.close();
        Record newRecord = record.copy();
        newRecord.setValue(lastRecordBatch);
        collect(newRecord);
        LOG.debug("Collected an arrow record batch {}", lastRecordBatch);
        rowCount = 0;
        rowBuffer.clear();
      }
      lastWriteTimeNs = System.nanoTime();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputOperator.getTypeInfo();
  }

  @Override
  public void close() {
    if (thread != null) {
      thread.interrupt();
    }
  }

  public int getBatchSize() {
    return batchSize;
  }

  public long getTimeoutMilliseconds() {
    return timeoutMilliseconds;
  }

  /** An ArrowRecordBatch that has schema information */
  public static class ArrowRecordBatchWithSchema extends ArrowRecordBatch {
    private final Schema schema;
    private boolean closed = false;

    public ArrowRecordBatchWithSchema(Schema schema, ArrowRecordBatch batch) {
      super(batch.getLength(), batch.getNodes(), batch.getBuffers());
      this.schema = schema;
    }

    public Schema getSchema() {
      return schema;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        super.close();
      }
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
