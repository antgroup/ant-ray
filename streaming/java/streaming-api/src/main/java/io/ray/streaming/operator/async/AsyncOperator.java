package io.ray.streaming.operator.async;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.async.AsyncFunction;
import io.ray.streaming.api.function.async.ResultFuture;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.async.AsyncQueue.OrderType;
import io.ray.streaming.util.ReflectionUtils;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeUtils;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

/**
 * AsyncOperator enables to process stream records asynchronously.
 *
 * @param <T> Type of the input data.
 * @param <R> Type of the output data.
 */
public class AsyncOperator<T, R> extends AbstractStreamOperator<AsyncFunction<T, R>>
    implements OneInputOperator<T> {

  /** Queue's capacity. */
  private final int capacity;
  /** Timeout in milliseconds. */
  private final long timeout;

  private final OrderType type;
  private final TypeInfo<T> inputTypeInfo;
  private transient AsyncQueue<T, R> queue;
  private transient AsyncElement<T, R> pendingElement;
  private transient ExecutorService commonExecutor;
  private transient ScheduledExecutorService scheduledExecutor;
  private transient Emitter<R> emitter;
  private transient Thread runner;

  public AsyncOperator(
      AsyncFunction<T, R> asyncFunction, OrderType type, int capacity, long timeout) {
    super(asyncFunction);
    setChainStrategy(ChainStrategy.ALWAYS);
    Preconditions.checkNotNull(type);
    Preconditions.checkArgument(capacity > 0, "capacity must be greater than 0");
    Preconditions.checkArgument(timeout > 0, "timeout must be greater than 0");
    this.type = type;
    this.capacity = capacity;
    this.timeout = timeout;

    Type[] paramTypes = TypeUtils.getParamTypes(AsyncFunction.class, function, "runAsync");
    Type resultFutureType = paramTypes[1];
    Type collectionType =
        ReflectionUtils.findMethod(ResultFuture.class, "complete").getGenericParameterTypes()[0];
    Type collectionGenericType = TypeUtils.resolveType(resultFutureType, collectionType);
    this.typeInfo = new TypeInfo(TypeUtils.resolveCollectionElemType(collectionGenericType));
    Type inputType = paramTypes[0];
    this.inputTypeInfo = new TypeInfo<>(inputType);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    super.open(collectorList, runtimeContext);
    initThreadPools();
    createQueue();
    this.emitter = new Emitter<>(queue, collectorList);
    this.runner = new Thread(emitter);
    this.runner.setDaemon(true);
    this.runner.start();
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    AsyncElement<T, R> element = new AsyncElement(record);

    // 1. register a timer future which executes the timeout method when timeout
    ScheduledFuture timerFuture =
        scheduledExecutor.schedule(
            () -> {
              function.timeout(record.getValue(), element);
            },
            timeout,
            TimeUnit.MILLISECONDS);

    // 2. cancel the timer future if async element finished
    element.onCompleted(
        ae -> {
          timerFuture.cancel(true);
        },
        commonExecutor);

    // 3. add the element to the queue
    addElement(element);

    // 4. execute the user defined function
    function.runAsync(record.getValue(), element);
  }

  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return inputTypeInfo;
  }

  @Override
  public void close() {
    super.close();
    emitter.stop();
    runner.interrupt();
    commonExecutor.shutdown();
    scheduledExecutor.shutdownNow();
  }

  private void initThreadPools() {
    commonExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadFactoryBuilder().setNameFormat("async-operator-common-pool-%d").build(),
            new AbortPolicy());
    scheduledExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder().setNameFormat("async-operator-scheduled-pool-%d").build());
  }

  private void createQueue() {
    switch (type) {
      case ORDERED:
        queue = new OrderedAsyncQueue<>(capacity, commonExecutor);
        break;
      case UNORDERED:
        queue = new UnorderedAsyncQueue<>(capacity, commonExecutor);
        break;
      default:
        throw new RuntimeException("Doesn't support type=" + type + " queue.");
    }
  }

  private void addElement(AsyncElement<T, R> element) throws Exception {
    pendingElement = element;
    queue.add(element);
    pendingElement = null;
  }
}
