package io.ray.streaming.util;

import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.chain.ChainedOneInputOperator;
import io.ray.streaming.operator.chain.ChainedOperator;
import io.ray.streaming.operator.chain.ChainedSourceOperator;
import io.ray.streaming.operator.chain.ChainedTwoInputOperator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OperatorUtil {

  public static Set<StreamOperator> generateTailOperators(List<StreamOperator> operators) {
    Set<StreamOperator> tailOperators = new HashSet<>();
    operators.forEach(
        operator -> {
          if (operator.getNextOperators().size() == 0) {
            tailOperators.add(operator);
          }
          operator
              .getNextOperators()
              .forEach(
                  subOperator -> {
                    if (!operators.contains(subOperator)) {
                      tailOperators.add(operator);
                    }
                  });
        });
    return tailOperators;
  }

  public static void createName(
      List<StreamOperator> operators, StreamOperator currOperator, StringBuilder name) {
    name.append(currOperator.getClass().getSimpleName());
    List<StreamOperator> subOperator =
        currOperator.getNextOperators().stream()
            .filter(operator -> operators.contains(operator))
            .collect(Collectors.toList());
    if (subOperator.size() > 0) {
      name.append("-{");
      for (int i = 0; i < subOperator.size(); i++) {
        if (i > 0) {
          name.append("-");
        }
        createName(operators, subOperator.get(i), name);
      }
      name.append("}");
    }
  }

  public static ChainedOperator newChainedOperator(
      List<StreamOperator> operators,
      List<Map<String, String>> configs,
      List<Map<String, Double>> resources) {
    switch (operators.get(0).getOpType()) {
      case SOURCE:
        return new ChainedSourceOperator(operators, configs, resources);
      case ONE_INPUT:
        return new ChainedOneInputOperator(operators, configs, resources);
      case TWO_INPUT:
        return new ChainedTwoInputOperator(operators, configs, resources);
      default:
        throw new IllegalArgumentException(
            "Unsupported operator type " + operators.get(0).getOpType());
    }
  }
}
