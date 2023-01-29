package io.ray.streaming.util;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.fury.types.TypeInference;
import io.ray.fury.util.Tuple2;
import io.ray.runtime.util.LambdaUtils;
import io.ray.streaming.api.collector.Collector;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.Schema;

public class TypeUtils {

  public static Type[] getParamTypes(
      Class<?> functionInterface, Serializable function, String methodName) {
    Method method = getFunctionMethod(functionInterface, function, methodName);
    if (isLambda(function.getClass())) {
      // Note lambda ImplMethod doesn't reserve generics.
      SerializedLambda lambda = LambdaUtils.getSerializedLambda(function);
      org.objectweb.asm.Type[] argumentTypes =
          org.objectweb.asm.Type.getType(lambda.getInstantiatedMethodType()).getArgumentTypes();
      return Arrays.stream(argumentTypes)
          .map(
              t -> {
                try {
                  return Class.forName(
                      t.getClassName(), false, function.getClass().getClassLoader());
                } catch (ClassNotFoundException e) {
                  throw new RuntimeException(
                      String.format(
                          "Class %s not found in class loader %s",
                          t.getClassName(), function.getClass().getClassLoader()));
                }
              })
          .toArray(Type[]::new);
    } else {
      return method.getGenericParameterTypes();
    }
  }

  public static Method getFunctionMethod(
      Class<?> functionInterface, Serializable function, String methodName) {
    if (isLambda(function.getClass())) {
      // Note lambda ImplMethod doesn't reserve generics.
      SerializedLambda lambda = LambdaUtils.getSerializedLambda(function);
      String methodDefClassName = lambda.getImplClass().replace("/", ".");
      try {
        Class<?> clz =
            Class.forName(methodDefClassName, false, function.getClass().getClassLoader());
        Optional<Method> methodOptional =
            ReflectionUtils.findMethods(clz, lambda.getImplMethodName()).stream()
                .filter(
                    m ->
                        org.objectweb.asm.Type.getType(m)
                            .getDescriptor()
                            .equals(lambda.getImplMethodSignature()))
                .findAny();

        return methodOptional.orElseThrow(
            () ->
                new RuntimeException(
                    String.format(
                        "Class %s doesn't have method %s with signature %s",
                        methodDefClassName,
                        lambda.getImplMethodName(),
                        lambda.getImplMethodSignature())));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unreachable");
      }
    }

    Method superClassMethod = ReflectionUtils.findMethod(functionInterface, methodName);
    List<Method> methodsList = ReflectionUtils.findMethods(function.getClass(), methodName);
    Method method = null;
    for (Method m : methodsList) {
      if (method == null && !Modifier.isAbstract(m.getModifiers())) {
        method = m;
      }
      if (method != null) {
        // generic method will have two methods returned by `getDeclaredMethods`
        // One has generics and the other not. We need the one with generics.
        boolean moreSpecific = true;
        for (int i = 0; i < superClassMethod.getParameterCount(); i++) {
          if (!method.getParameterTypes()[i].isAssignableFrom(m.getParameterTypes()[i])) {
            moreSpecific = false;
            break;
          }
        }
        if (moreSpecific) {
          method = m;
        }
      }
    }
    Preconditions.checkArgument(method != null);
    return method;
  }

  /** Returns true if the specified class is a lambda. */
  public static boolean isLambda(Class clz) {
    Preconditions.checkNotNull(clz);
    return clz.getName().indexOf('/') >= 0;
  }

  public static Method getSam(Class<?> clz) {
    Preconditions.checkArgument(
        clz.isInterface(), "Class %s has more than one abstract method.", clz);
    Method sam = null;
    for (Method method : clz.getMethods()) {
      if (Modifier.isAbstract(method.getModifiers())) {
        if (sam == null) {
          sam = method;
        } else {
          throw new IllegalArgumentException(
              String.format("Class %s has more than one abstract method.", clz));
        }
      }
    }
    return sam;
  }

  public static Type resolveCollectorValueType(Type collectorType) {
    Method method = ReflectionUtils.findMethod(Collector.class, "collect");
    return resolveType(collectorType, method.getGenericParameterTypes()[0]);
  }

  public static Type resolveType(Type baseType, Type type) {
    return TypeToken.of(baseType).resolveType(type).getType();
  }

  public static Type resolveCollectionElemType(Type collectionType) {
    return TypeInference.getElementType(TypeToken.of(collectionType)).getType();
  }

  public static Tuple2<Schema, String> tryInferSchema(Type type) {
    try {
      Schema schema = TypeInference.inferSchema(type);
      return Tuple2.of(schema, null);
    } catch (Exception e) {
      return Tuple2.of(null, e.getMessage());
    }
  }
}
