package io.ray.api.options;

import java.util.ArrayList;
import java.util.List;

/**
 * Actor affinity scheduling strategy. Scheduling actor to a node that satisfies a set of affinity
 * expressions. eg: scheduling into node of actor which the value of label key "location" is in
 * {"dc-1", "dc-2"} and exists label key "version".
 *
 * <p>List<> locationValues = new ArrayList<>() {{ add("dc_1"); add("dc_2");}};
 * ActorAffinitySchedulingStrategy schedulingStrategy = new
 * ActorAffinitySchedulingStrategy.Builder()
 * .addExpression(ActorAffinityMatchExpression.in("location", locationValues, false))
 * .addExpression(ActorAffinityMatchExpression.exists("version", true)) .build(); ActorHandle<>
 * actor = Ray.actor(Counter::new, 1) .setSchedulingStrategy(schedulingStrategy).remote();
 * actor2.task(Counter::getValue).remote().get();
 */
public class ActorAffinitySchedulingStrategy implements SchedulingStrategy {
  // a list of match expression
  public final List<ActorAffinityMatchExpression> actorAffinityMatchExpressions;

  private ActorAffinitySchedulingStrategy(List<ActorAffinityMatchExpression> expressions) {
    this.actorAffinityMatchExpressions = expressions;
  }

  /** The inner class for building ActorAffinitySchedulingStrategy. */
  public static class Builder {
    private List<ActorAffinityMatchExpression> actorAffinityMatchExpressions = new ArrayList<>();

    /*
     * add actor affinity match expression
     * It can be called multiple times,
     * and the matching mechanism requires all expressions to be satisfied.
     */
    public Builder addExpression(ActorAffinityMatchExpression expression) {
      this.actorAffinityMatchExpressions.add(expression);
      return this;
    }

    // build ActorAffinitySchedulingStrategy
    public ActorAffinitySchedulingStrategy build() {
      return new ActorAffinitySchedulingStrategy(actorAffinityMatchExpressions);
    }
  }
}
