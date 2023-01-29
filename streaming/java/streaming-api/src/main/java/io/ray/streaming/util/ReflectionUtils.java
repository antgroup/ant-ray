package io.ray.streaming.util;

import com.google.common.base.Preconditions;
import io.ray.api.function.RayFunc1;
import io.ray.api.function.RayFuncVoid1;
import io.ray.runtime.util.LambdaUtils;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

@SuppressWarnings("UnstableApiUsage")
public class ReflectionUtils {

  public static Method findMethod(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    Preconditions.checkArgument(methods.size() == 1);
    return methods.get(0);
  }

  /**
   * For covariant return type, return the most specific method.
   *
   * @return all methods named by {@code methodName},
   */
  public static List<Method> findMethods(Class<?> cls, String methodName) {
    List<Class<?>> classes = new ArrayList<>();
    Class<?> clazz = cls;
    while (clazz != null) {
      classes.add(clazz);
      clazz = clazz.getSuperclass();
    }
    classes.addAll(getAllInterfaces(cls));
    if (classes.indexOf(Object.class) == -1) {
      classes.add(Object.class);
    }

    LinkedHashMap<List<Class<?>>, Method> methods = new LinkedHashMap<>();
    for (Class<?> superClass : classes) {
      for (Method m : superClass.getDeclaredMethods()) {
        if (m.getName().equals(methodName)) {
          List<Class<?>> params = Arrays.asList(m.getParameterTypes());
          Method method = methods.get(params);
          if (method == null) {
            methods.put(params, m);
          } else {
            // for covariant return type, use the most specific method
            if (method.getReturnType() != m.getReturnType()
                && method.getReturnType().isAssignableFrom(m.getReturnType())) {
              methods.put(params, m);
            }
          }
        }
      }
    }
    return new ArrayList<>(methods.values());
  }

  /**
   * Gets a <code>List</code> of all interfaces implemented by the given class and its superclasses.
   *
   * <p>The order is determined by looking through each interface in turn as declared in the source
   * file and following its hierarchy up.
   */
  public static List<Class<?>> getAllInterfaces(Class<?> cls) {
    if (cls == null) {
      return null;
    }

    LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
    getAllInterfaces(cls, interfacesFound);
    return new ArrayList<>(interfacesFound);
  }

  private static void getAllInterfaces(Class<?> cls, LinkedHashSet<Class<?>> interfacesFound) {
    while (cls != null) {
      Class[] interfaces = cls.getInterfaces();
      for (Class anInterface : interfaces) {
        if (!interfacesFound.contains(anInterface)) {
          interfacesFound.add(anInterface);
          getAllInterfaces(anInterface, interfacesFound);
        }
      }

      cls = cls.getSuperclass();
    }
  }

  /** Returns true if the specified class is a lambda. */
  public static boolean isLambda(Class clz) {
    Preconditions.checkNotNull(clz);
    return clz.getName().indexOf('/') >= 0;
  }

  public static <T0, R> String getMethodName(RayFunc1<T0, R> function) {
    return getMethodNameInternal(function);
  }

  public static <T> String getMethodName(RayFuncVoid1<T> function) {
    return getMethodNameInternal(function);
  }

  public static String getMethodNameInternal(Serializable function) {
    Preconditions.checkArgument(isLambda(function.getClass()));
    return LambdaUtils.getSerializedLambda(function).getImplMethodName();
  }
}
