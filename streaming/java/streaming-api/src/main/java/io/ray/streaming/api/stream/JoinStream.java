package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.api.partition.impl.KeyPartition;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.impl.JoinOperator;
import io.ray.streaming.operator.impl.KeySelectorOperator;
import io.ray.streaming.operator.impl.PkJoinOperator;
import io.ray.streaming.util.TypeInfo;
import io.ray.streaming.util.TypeResolver;
import java.io.Serializable;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Represents a DataStream of two joined DataStream.
 *
 * @param <L> Type of the data in the left stream.
 * @param <R> Type of the data in the right stream.
 * @param <O> Type of the data in the joined stream.
 */
public class JoinStream<L, R, O> extends DataStream<L> {
  private DataStream<R> rightStream;

  public JoinStream(DataStream<L> leftStream, DataStream<R> rightStream) {
    super(leftStream, new JoinOperator<>());
    this.rightStream = rightStream;
  }

  public DataStream<R> getRightStream() {
    return rightStream;
  }

  /** Apply key-by to the left join stream. */
  public <K> Where<K> where(KeyFunction<L, K> keyFunction) {
    return new Where<>(this, keyFunction);
  }

  public <K, P> WhereWithPK where(
      KeyFunction<L, K> leftKeySelector, KeyFunction<L, P> leftPKExtractor) {
    return new WhereWithPK<>(this, leftKeySelector, leftPKExtractor);
  }

  /**
   * Where clause of the join transformation.
   *
   * @param <K> Type of the join key.
   */
  public class Where<K> implements Serializable {
    protected JoinStream<L, R, O> joinStream;

    Where(JoinStream<L, R, O> joinStream, KeyFunction<L, K> leftKeyByFunction) {
      this.joinStream = joinStream;
      KeySelectorOperator<L, K> keySelectorOperator = new KeySelectorOperator<>(leftKeyByFunction);
      keySelectorOperator.setName("");
      inputStream = new DataStream((DataStream) getInputStream(), keySelectorOperator);
      inputStream.setPartition(new KeyPartition());
    }

    public Equal<K> equal(KeyFunction<R, K> rightKeyFunction) {
      return new Equal(joinStream, rightKeyFunction);
    }
  }

  /** Where clause of the join transformation. */
  public class WhereWithPK<K, P> extends Where<K> {

    private final KeyFunction<L, P> leftPKExtractor;

    WhereWithPK(
        JoinStream<L, R, O> joinDataStream,
        KeyFunction<L, K> leftKeySelector,
        KeyFunction<L, P> leftPKExtractor) {
      super(joinDataStream, leftKeySelector);
      this.leftPKExtractor = leftPKExtractor;
    }

    public <Q> EqualToWithPK<K, P, Q> equal(
        KeyFunction<R, K> rightKeySelector, KeyFunction<R, Q> rightPKExtractor) {
      return new EqualToWithPK(joinStream, rightKeySelector, leftPKExtractor, rightPKExtractor);
    }
  }

  /**
   * Equal clause of the join transformation.
   *
   * @param <K> Type of the join key.
   */
  public class Equal<K> implements Serializable {
    private JoinStream<L, R, O> joinStream;
    protected Class<?>[] typeArguments;

    Equal(JoinStream<L, R, O> joinStream, KeyFunction<R, K> rightKeyByFunction) {
      this.joinStream = joinStream;
      KeySelectorOperator<R, K> otherBatchKeySelectorOperator =
          new KeySelectorOperator<>(rightKeyByFunction);
      otherBatchKeySelectorOperator.setName("");
      rightStream = new DataStream<R>(rightStream, otherBatchKeySelectorOperator);
      rightStream.setPartition(new KeyPartition());
    }

    @SuppressWarnings("unchecked")
    public <O> DataStream<O> with(JoinFunction<L, R, O> joinFunction) {
      JoinOperator joinOperator = (JoinOperator) joinStream.getOperator();
      joinOperator.setFunction(joinFunction);
      return (DataStream<O>) joinStream;
    }
  }

  @Override
  public JoinStream<L, R, O> disableChain() {
    return withChainStrategy(ChainStrategy.NEVER);
  }

  @Override
  public JoinStream<L, R, O> withChainStrategy(ChainStrategy chainStrategy) {
    super.withChainStrategy(chainStrategy);
    return this;
  }

  @Override
  public JoinStream<L, R, O> forward() {
    super.forward();
    return this;
  }

  @Override
  public JoinStream<L, R, O> setParallelism(int parallelism) {
    super.setParallelism(parallelism);
    return this;
  }

  @Override
  public JoinStream<L, R, O> withSchema(Schema schema) {
    super.withSchema(schema);
    return this;
  }

  @Override
  public JoinStream<L, R, O> withType(TypeInfo typeInfo) {
    super.withType(typeInfo);
    return this;
  }

  @Override
  public JoinStream<L, R, O> setDynamicDivisionNum(int dynamicDivisionNum) {
    throw new UnsupportedOperationException("This function is undefined");
  }

  @Override
  public JoinStream<L, R, O> withConfig(Map<String, String> config) {
    super.withConfig(config);
    return this;
  }

  @Override
  public JoinStream<L, R, O> withConfig(String key, String value) {
    super.withConfig(key, value);
    return this;
  }

  @Override
  public JoinStream<L, R, O> withResource(String resourceKey, Double resourceValue) {
    super.withResource(resourceKey, resourceValue);
    return this;
  }

  @Override
  public JoinStream<L, R, O> withResource(Map<String, Double> resources) {
    super.withResource(resources);
    return this;
  }

  public class EqualToWithPK<K, P, Q> extends Equal<K> {

    private final KeyFunction<L, P> primaryKeyExtractor1;
    private final KeyFunction<R, Q> primaryKeyExtractor2;
    private JoinStream<L, R, O> joinStream;

    EqualToWithPK(
        JoinStream<L, R, O> joinDataStream,
        KeyFunction<R, K> keySelector2,
        KeyFunction<L, P> primaryKeyExtractor1,
        KeyFunction<R, Q> primaryKeyExtractor2) {
      super(joinDataStream, keySelector2);
      this.joinStream = joinDataStream;
      typeArguments = new Class[6];
      Class<?>[] keySelectorTypeArguments =
          TypeResolver.resolveRawArguments(KeyFunction.class, keySelector2.getClass());
      typeArguments[0] = keySelectorTypeArguments[1];

      this.primaryKeyExtractor1 = primaryKeyExtractor1;
      this.primaryKeyExtractor2 = primaryKeyExtractor2;
      Class<?>[] leftPKSelectorTypeArguments =
          TypeResolver.resolveRawArguments(KeyFunction.class, this.primaryKeyExtractor1.getClass());
      typeArguments[4] = leftPKSelectorTypeArguments[1];

      Class<?>[] rightPKSelectorTypeArguments =
          TypeResolver.resolveRawArguments(KeyFunction.class, this.primaryKeyExtractor2.getClass());
      typeArguments[5] = rightPKSelectorTypeArguments[1];
    }

    @SuppressWarnings("unchecked")
    public <O> DataStream<O> with(JoinFunction<L, R, O> function) {
      JoinOperator joinOperator = (JoinOperator) joinStream.getOperator();
      joinOperator.setFunction(function);
      Class<?>[] joinTypeArguments =
          TypeResolver.resolveRawArguments(JoinFunction.class, function.getClass());
      typeArguments[1] = joinTypeArguments[0];
      typeArguments[2] = joinTypeArguments[1];
      typeArguments[3] = joinTypeArguments[2];
      setOperator(
          new PkJoinOperator<>(
              function, primaryKeyExtractor1, primaryKeyExtractor2, typeArguments));
      return (DataStream<O>) joinStream;
    }
  }
}
