package io.ray.api.options;

import java.util.ArrayList;
import java.util.List;

// An expression to match labels, to achieve actor affinity.
public class ActorAffinityMatchExpression {
  private String key;
  private ActorAffinityOperator operator;
  private List<String> values;
  private boolean isSoft;

  private ActorAffinityMatchExpression(
      String key, ActorAffinityOperator operator, List<String> values, boolean isSoft) {
    this.key = key;
    this.operator = operator;
    this.values = values;
    this.isSoft = isSoft;
  }

  public String getKey() {
    return key;
  }

  public ActorAffinityOperator getOperator() {
    return operator;
  }

  public List<String> getValues() {
    return values;
  }

  public boolean isSoft() {
    return isSoft;
  }

  /**
   * Returns an affinity expression to indicate that the target actor is expected to be scheduled
   * with the actors whose label meets one of the composed key and values. eg:
   * ActorAffinityMatchExpression.in("location", new ArrayList<>() {{ add("dc-1");}}, false).
   *
   * @param key The key of label.
   * @param values A list of label values.
   * @param isSoft If true, the actor will be scheduled even there's no matched actor.
   * @return ActorAffinityMatchExpression.
   */
  public static ActorAffinityMatchExpression in(String key, List<String> values, boolean isSoft) {
    return new ActorAffinityMatchExpression(key, ActorAffinityOperator.IN, values, isSoft);
  }

  /**
   * Returns an affinity expression to indicate that the target actor is not expected to be
   * scheduled with the actors whose label meets one of the composed key and values. eg:
   * ActorAffinityMatchExpression.notIn( "location", new ArrayList<>() {{ add("dc-1");}}, false).
   *
   * @param key The key of label.
   * @param values A list of label values.
   * @param isSoft If true, the actor will be scheduled even there's no matched actor.
   * @return ActorAffinityMatchExpression.
   */
  public static ActorAffinityMatchExpression notIn(
      String key, List<String> values, boolean isSoft) {
    return new ActorAffinityMatchExpression(key, ActorAffinityOperator.NOT_IN, values, isSoft);
  }

  /**
   * Returns an affinity expression to indicate that the target actor is expected to be scheduled
   * with the actors whose labels exists the specified key. eg:
   * ActorAffinityMatchExpression.exists("location", false).
   *
   * @param key The key of label.
   * @param isSoft If true, the actor will be scheduled even there's no matched actor.
   * @return ActorAffinityMatchExpression.
   */
  public static ActorAffinityMatchExpression exists(String key, boolean isSoft) {
    return new ActorAffinityMatchExpression(
        key, ActorAffinityOperator.EXISTS, new ArrayList<String>(), isSoft);
  }

  /**
   * Returns an affinity expression to indicate that the target actor is not expected to be
   * scheduled with the actors whose labels exists the specified key. eg:
   * ActorAffinityMatchExpression.doesNotExist("location", false).
   *
   * @param key The key of label.
   * @param isSoft If true, the actor will be scheduled even there's no matched actor.
   * @return ActorAffinityMatchExpression.
   */
  public static ActorAffinityMatchExpression doesNotExist(String key, boolean isSoft) {
    return new ActorAffinityMatchExpression(
        key, ActorAffinityOperator.DOES_NOT_EXIST, new ArrayList<String>(), isSoft);
  }
}
