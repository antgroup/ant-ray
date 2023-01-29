package io.ray.streaming.util;

import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.RichFunction;

public class FunctionUtils {
  public static void openFunction(Function function, RuntimeContext runtimeContext) {
    if (function instanceof RichFunction) {
      RichFunction richFunction = (RichFunction) function;
      richFunction.open(runtimeContext);
    }
  }

  public static void closeFunction(Function function) {
    if (function instanceof RichFunction) {
      RichFunction richFunction = (RichFunction) function;
      richFunction.close();
    }
  }
}
