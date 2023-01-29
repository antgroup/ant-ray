package io.ray.runtime.functionmanager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.ray.api.function.RayFunc;
import io.ray.runtime.util.LambdaUtils;
import java.io.File;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages functions. */
public class FunctionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManager.class);

  static final String CONSTRUCTOR_NAME = "<init>";

  /**
   * Cache from a RayFunc object to its corresponding JavaFunctionDescriptor. Because
   * `LambdaUtils.getSerializedLambda` is expensive.
   */
  // If the cache is not thread local, we'll need a lock to protect it,
  // which means competition is highly possible.
  private static final ThreadLocal<HashMap<Class<? extends RayFunc>, JavaFunctionDescriptor>>
      RAY_FUNC_CACHE = ThreadLocal.withInitial(HashMap::new);

  /** The functions that belong to this job. */
  private JobFunctionTable jobFunctionTable;

  /** The resource path which we can load the job's jar resources. */
  private final List<String> codeSearchPath;

  /**
   * Construct a FunctionManager with the specified code search path.
   *
   * @param codeSearchPath The specified job resource that can store the job's resources.
   */
  public FunctionManager(List<String> codeSearchPath) {
    Preconditions.checkNotNull(codeSearchPath);
    this.codeSearchPath = codeSearchPath;
    if (!codeSearchPath.isEmpty()) {
      LOGGER.debug("Created FunctionManager from code search path {}", codeSearchPath);
    }
  }

  /**
   * Get the RayFunction from a RayFunc instance (a lambda).
   *
   * @param func The lambda.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(RayFunc func) {
    JavaFunctionDescriptor functionDescriptor = RAY_FUNC_CACHE.get().get(func.getClass());
    if (functionDescriptor == null) {
      // It's OK to not lock here, because it's OK to have multiple JavaFunctionDescriptor instances
      // for the same RayFunc instance.
      SerializedLambda serializedLambda = LambdaUtils.getSerializedLambda(func);
      final String className = serializedLambda.getImplClass().replace('/', '.');
      final String methodName = serializedLambda.getImplMethodName();
      final String signature = serializedLambda.getImplMethodSignature();
      functionDescriptor = new JavaFunctionDescriptor(className, methodName, signature);
      RAY_FUNC_CACHE.get().put(func.getClass(), functionDescriptor);
    }
    return getFunction(functionDescriptor);
  }

  /**
   * Get the RayFunction from a function descriptor.
   *
   * @param functionDescriptor The function descriptor.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(JavaFunctionDescriptor functionDescriptor) {
    if (jobFunctionTable == null) {
      synchronized (this) {
        if (jobFunctionTable == null) {
          jobFunctionTable = createJobFunctionTable();
        }
      }
    }
    return jobFunctionTable.getFunction(functionDescriptor);
  }

  private JobFunctionTable createJobFunctionTable() {
    ClassLoader classLoader;
    if (codeSearchPath.isEmpty()) {
      classLoader = getClass().getClassLoader();
    } else {
      URL[] urls =
          codeSearchPath.stream()
              .filter(StringUtils::isNotBlank)
              .flatMap(
                  p -> {
                    try {
                      List<URL> subUrls = new ArrayList<>();
                      if (!Files.isDirectory(Paths.get(p)) && p.endsWith(".jar")) {
                        subUrls.add(Paths.get(p).toAbsolutePath().toUri().toURL());
                      } else {
                        Path dir = Paths.get(p);
                        if (!Files.isDirectory(dir)) {
                          dir = dir.getParent();
                        }
                        subUrls.add(dir.toAbsolutePath().toUri().toURL());
                        File[] jars = dir.toFile().listFiles((d, name) -> name.endsWith(".jar"));
                        if (jars != null) {
                          for (File jar : jars) {
                            subUrls.add(jar.toPath().toUri().toURL());
                          }
                        }
                      }
                      return subUrls.stream();
                    } catch (Exception e) {
                      throw new RuntimeException(String.format("Illegal '%s' resource path", p));
                    }
                  })
              .toArray(URL[]::new);
      classLoader = new URLClassLoader(urls);
      LOGGER.info("Resource loaded for from path '{}'.", urls);
    }

    return new JobFunctionTable(classLoader);
  }

  /** Manages all functions that belong to one driver. */
  static class JobFunctionTable {

    /** The job's corresponding class loader. */
    final ClassLoader classLoader;

    private final List<URL> paths;
    /** Functions per class, per function name + type descriptor. */
    ConcurrentMap<String, Map<Pair<String, String>, RayFunction>> functions;

    JobFunctionTable(ClassLoader classLoader) {
      this.classLoader = classLoader;
      if (classLoader instanceof URLClassLoader) {
        this.paths = Arrays.asList(((URLClassLoader) classLoader).getURLs());
      } else {
        this.paths = new ArrayList<>();
      }
      this.functions = new ConcurrentHashMap<>();
    }

    RayFunction getFunction(JavaFunctionDescriptor descriptor) {
      Map<Pair<String, String>, RayFunction> classFunctions = functions.get(descriptor.className);
      if (classFunctions == null) {
        synchronized (this) {
          classFunctions = functions.get(descriptor.className);
          if (classFunctions == null) {
            classFunctions = loadFunctionsForClass(descriptor.className);
            functions.put(descriptor.className, classFunctions);
          }
        }
      }
      final Pair<String, String> key = ImmutablePair.of(descriptor.name, descriptor.signature);
      RayFunction func = classFunctions.get(key);
      if (func == null) {
        if (classFunctions.containsKey(key)) {
          throw new RuntimeException(
              String.format(
                  "RayFunction %s is overloaded, the signature can't be empty.",
                  descriptor.toString()));
        } else {
          String msg;
          if (!paths.isEmpty()) {
            msg = String.format("RayFunction %s not found from paths %s", descriptor, paths);
          } else {
            msg =
                String.format(
                    "RayFunction %s not found from classloader %s", descriptor, classLoader);
          }
          throw new RuntimeException(msg);
        }
      }
      return func;
    }

    /** Load all functions from a class. */
    Map<Pair<String, String>, RayFunction> loadFunctionsForClass(String className) {
      // If RayFunction is null, the function is overloaded.
      Map<Pair<String, String>, RayFunction> map = new HashMap<>();
      try {
        Class clazz = Class.forName(className, true, classLoader);
        List<Executable> executables = new ArrayList<>();
        executables.addAll(Arrays.asList(clazz.getDeclaredMethods()));
        executables.addAll(Arrays.asList(clazz.getDeclaredConstructors()));

        Class clz = clazz;
        clz = clz.getSuperclass();
        while (clz != null && clz != Object.class) {
          executables.addAll(Arrays.asList(clz.getDeclaredMethods()));
          clz = clz.getSuperclass();
        }

        // Put interface methods ahead, so that in can be override by subclass methods in `map.put`
        for (Class baseInterface : clazz.getInterfaces()) {
          for (Method method : baseInterface.getDeclaredMethods()) {
            if (method.isDefault()) {
              executables.add(method);
            }
          }
        }

        // Use reverse order so that child class methods can override super class methods.
        for (Executable e : Lists.reverse(executables)) {
          e.setAccessible(true);
          final String methodName = e instanceof Method ? e.getName() : CONSTRUCTOR_NAME;
          final Type type =
              e instanceof Method ? Type.getType((Method) e) : Type.getType((Constructor) e);
          final String signature = type.getDescriptor();
          RayFunction rayFunction =
              new RayFunction(
                  e, classLoader, new JavaFunctionDescriptor(className, methodName, signature));
          map.put(ImmutablePair.of(methodName, signature), rayFunction);
          // For cross language call java function without signature
          final Pair<String, String> emptyDescriptor = ImmutablePair.of(methodName, "");
          if (map.containsKey(emptyDescriptor)) {
            map.put(emptyDescriptor, null); // Mark this function as overloaded.
          } else {
            map.put(emptyDescriptor, rayFunction);
          }
        }
      } catch (Exception e) {
        String msg;
        if (!paths.isEmpty()) {
          msg =
              String.format("Failed to load functions of class %s from paths %s", className, paths);
        } else {
          msg =
              String.format(
                  "Failed to load functions of class %s from classloader %s",
                  className, classLoader);
        }
        throw new RuntimeException(msg, e);
      }
      return map;
    }
  }
}
