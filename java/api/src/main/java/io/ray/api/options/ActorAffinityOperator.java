package io.ray.api.options;

/*
 * the strategy of match special actor
 */
public enum ActorAffinityOperator {

  // value of label key is in this list of values
  IN(0),

  // value of label key is not in this list of values
  NOT_IN(1),

  // exists the label key
  EXISTS(2),

  // does not exists the label key
  DOES_NOT_EXIST(3),

  /** Unrecognized operator. */
  UNRECOGNIZED(-1);

  private int value = 0;

  ActorAffinityOperator(int value) {
    this.value = value;
  }

  // return value
  public int value() {
    return this.value;
  }
}
