package io.ray.test;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.ray.api.ActorHandle;
import io.ray.api.CppActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.function.CppActorClass;
import io.ray.api.function.CppActorMethod;
import io.ray.api.function.CppFunction;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.api.options.ActorAffinityMatchExpression;
import io.ray.api.options.ActorAffinitySchedulingStrategy;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.exception.CrossLanguageException;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.serializer.RayExceptionSerializer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class CrossLanguageInvocationTest extends BaseTest {

  private static final String PYTHON_MODULE = "test_cross_language_invocation";
  private static final String[] CPP_LIBRARYS = {"counter", "plus"};

  private static class JavaCounter {

    private int value;

    // Constructor
    public JavaCounter(int initValue) {
      this.value = initValue;
    }

    // return value
    public int getValue() {
      return value;
    }
  }

  @BeforeClass
  public void beforeClass() {
    // Delete and re-create the temp dir.
    File tempDir =
        new File(System.getProperty("java.io.tmpdir") + File.separator + "ray_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in =
        CrossLanguageInvocationTest.class.getResourceAsStream("/" + PYTHON_MODULE + ".py");
    File pythonFile = new File(tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Write the test Cpp files to the temp dir.
    for (String lib : CPP_LIBRARYS) {
      in = CrossLanguageInvocationTest.class.getResourceAsStream("/" + lib + ".so");
      File cppFile = new File(tempDir.getAbsolutePath() + File.separator + lib + ".so");
      try {
        FileUtils.copyInputStreamToFile(in, cppFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + tempDir.getAbsolutePath());
  }

  @Test
  public void testCallingPythonFunction() {
    Object[] inputs =
        new Object[] {
          true, // Boolean
          Byte.MAX_VALUE, // Byte
          Short.MAX_VALUE, // Short
          Integer.MAX_VALUE, // Integer
          Long.MAX_VALUE, // Long
          // BigInteger can support max value of 2^64-1, please refer to:
          // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
          // If BigInteger larger than 2^64-1, the value can only be transferred among Java workers.
          BigInteger.valueOf(Long.MAX_VALUE), // BigInteger
          "Hello World!", // String
          1.234f, // Float
          1.234, // Double
          "example binary".getBytes()
        }; // byte[]
    for (Object o : inputs) {
      ObjectRef res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", o.getClass()), o).remote();
      Assert.assertEquals(res.get(), o);
    }
    // null
    {
      Object input = null;
      ObjectRef<Object> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", Object.class), input).remote();
      Object r = res.get();
      Assert.assertEquals(r, input);
    }
    // array
    {
      int[] input = new int[] {1, 2};
      ObjectRef<int[]> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", int[].class), input).remote();
      int[] r = res.get();
      Assert.assertEquals(r, input);
    }
    // array of Object
    {
      Object[] input =
          new Object[] {1, 2.3f, 4.56, "789", "10".getBytes(), null, true, new int[] {1, 2}};
      ObjectRef<Object[]> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", Object[].class), input).remote();
      Object[] r = res.get();
      // If we tell the value type is Object, then all numbers will be Number type.
      Assert.assertEquals(((Number) r[0]).intValue(), input[0]);
      Assert.assertEquals(((Number) r[1]).floatValue(), input[1]);
      Assert.assertEquals(((Number) r[2]).doubleValue(), input[2]);
      // String cast
      Assert.assertEquals((String) r[3], input[3]);
      // binary cast
      Assert.assertEquals((byte[]) r[4], input[4]);
      // null
      Assert.assertEquals(r[5], input[5]);
      // Boolean cast
      Assert.assertEquals((Boolean) r[6], input[6]);
      // array cast
      Object[] r7array = (Object[]) r[7];
      int[] input7array = (int[]) input[7];
      Assert.assertEquals(((Number) r7array[0]).intValue(), input7array[0]);
      Assert.assertEquals(((Number) r7array[1]).intValue(), input7array[1]);
    }
    // Unsupported types, all Java specific types, e.g. List / Map...
    {
      Assert.expectThrows(
          Exception.class,
          () -> {
            List<Integer> input = Arrays.asList(1, 2);
            ObjectRef<List<Integer>> res =
                Ray.task(
                        PyFunction.of(
                            PYTHON_MODULE,
                            "py_return_input",
                            (Class<List<Integer>>) input.getClass()),
                        input)
                    .remote();
            List<Integer> r = res.get();
            Assert.assertEquals(r, input);
          });
    }
  }

  @Test
  public void testPythonCallJavaFunction() {
    ObjectRef<String> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_call_java_function", String.class)).remote();
    Assert.assertEquals(res.get(), "success");
  }

  @Test
  public void testCallingPythonActor() {
    PyActorHandle actor =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    ObjectRef<byte[]> res =
        actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "2".getBytes());
  }

  @Test
  public void testCallingPythonAsyncActor() {
    {
      PyActorHandle actor =
          Ray.actor(PyActorClass.of(PYTHON_MODULE, "AsyncCounter"), "1".getBytes())
              .setAsync(true)
              .remote();
      actor.task(PyActorMethod.of("block_task", byte[].class)).remote();
      ObjectRef<byte[]> res =
          actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
      Assert.assertEquals(res.get(), "2".getBytes());
    }

    {
      PyActorHandle actor =
          Ray.actor(PyActorClass.of(PYTHON_MODULE, "NonAsyncCounter"), "1".getBytes())
              .setAsync(false)
              .remote();
      actor.task(PyActorMethod.of("block_task", byte[].class)).remote();
      ObjectRef<byte[]> res =
          actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();

      Supplier<Boolean> getValue =
          () -> {
            try {
              if (equals(res.get() == "2".getBytes())) {
                return true;
              } else {
                return false;
              }
            } catch (CrossLanguageException e) {
              return false;
            }
          };
      Assert.assertFalse(TestUtils.waitForCondition(getValue, 30000));
    }
  }

  @Test
  public void testCallingCppFunction() {
    ObjectRef<Integer> res = Ray.task(CppFunction.of("Plus", Integer.class), 1, 2).remote();
    Assert.assertEquals(res.get(), Integer.valueOf(3));

    ObjectRef<int[]> res1 =
        Ray.task(CppFunction.of("ReturnLargeArray", int[].class), new int[100000]).remote();
    Assert.assertEquals(res1.get().length, 100000);

    ObjectRef<String> res2 = Ray.task(CppFunction.of("Echo", String.class), "CallCpp").remote();
    Assert.assertEquals(res2.get(), "CallCpp");

    Assert.expectThrows(
        Exception.class,
        () -> {
          ObjectRef<Object> res3 = Ray.task(CppFunction.of("ThrowTask")).remote();
          res3.get();
        });
  }

  @Test
  public void testCallingCppActor() {
    String actorName = "actor_name";
    CppActorHandle actor =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setName(actorName)
            .remote();
    ObjectRef<Integer> res = actor.task(CppActorMethod.of("Plus1", Integer.class)).remote();
    Assert.assertEquals(res.get(), Integer.valueOf(1));
    ObjectRef<byte[]> b =
        actor.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.assertEquals(b.get(), "C++ Worker".getBytes());

    ObjectRef<byte[]> b2 =
        actor.task(CppActorMethod.of("echoBytes", byte[].class), "C++ Worker".getBytes()).remote();
    Assert.assertEquals(b2.get(), "C++ Worker".getBytes());

    // Test get cpp actor by actor name.
    Optional<CppActorHandle> optional = Ray.getActor(actorName);
    Assert.assertTrue(optional.isPresent());
    CppActorHandle actor2 = optional.get();
    ObjectRef<Integer> res2 = actor2.task(CppActorMethod.of("Plus1", Integer.class)).remote();
    Assert.assertEquals(res2.get(), Integer.valueOf(2));

    // Test get other cpp actor by actor name.
    String childName = "child_name";
    ObjectRef<String> res3 =
        actor.task(CppActorMethod.of("CreateNestedChildActor", String.class), childName).remote();
    Assert.assertEquals(res3.get(), "OK");
    Optional<CppActorHandle> optional3 = Ray.getActor(childName);
    Assert.assertTrue(optional3.isPresent());
    CppActorHandle actor3 = optional3.get();
    ObjectRef<Integer> res4 = actor3.task(CppActorMethod.of("Plus1", Integer.class)).remote();
    Assert.assertEquals(res4.get(), Integer.valueOf(1));
  }

  @Test
  public void testPythonCallJavaActor() {
    ObjectRef<byte[]> res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_java_actor", byte[].class),
                "1".getBytes())
            .remote();
    Assert.assertEquals(res.get(), "Counter1".getBytes());
  }

  @Test
  public void testPassActorHandleFromPythonToJava() {
    // Call a python function which creates a python actor
    // and pass the actor handle to callPythonActorHandle.
    ObjectRef<byte[]> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_pass_python_actor_handle", byte[].class))
            .remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  @Test
  public void testPassActorHandleFromJavaToPython() {
    // Create a java actor, and pass actor handle to python.
    ActorHandle<TestActor> javaActor = Ray.actor(TestActor::new, "1".getBytes()).remote();
    Preconditions.checkState(javaActor instanceof NativeActorHandle);
    ObjectRef<byte[]> res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_java_actor_from_handle", byte[].class),
                javaActor)
            .remote();
    Assert.assertEquals(res.get(), "12".getBytes());
    // Create a python actor, and pass actor handle to python.
    PyActorHandle pyActor =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    Preconditions.checkState(pyActor instanceof NativeActorHandle);
    res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_python_actor_from_handle", byte[].class),
                pyActor)
            .remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  @Test
  public void testExceptionSerialization() throws IOException {
    try {
      throw new RayException("Test Exception");
    } catch (RayException e) {
      String formattedException =
          org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e);
      io.ray.runtime.generated.Common.RayException exception =
          io.ray.runtime.generated.Common.RayException.parseFrom(RayExceptionSerializer.toBytes(e));
      Assert.assertEquals(exception.getFormattedExceptionString(), formattedException);
    }
  }

  @Test
  public void testRaiseExceptionFromPython() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_python_raise_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      // ex is a Python exception(py_func_python_raise_exception) with no cause.
      Assert.assertTrue(ex instanceof CrossLanguageException);
      CrossLanguageException e = (CrossLanguageException) ex;
      Assert.assertEquals(e.getLanguage(), Language.PYTHON);
      // ex.cause is null.
      Assert.assertNull(ex.getCause());
      Assert.assertTrue(
          ex.getMessage().contains("ZeroDivisionError: division by zero"), ex.getMessage());
      return;
    }
    Assert.fail();
  }

  @Test
  public void testThrowExceptionFromJava() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_java_throw_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_java_throw_exception"), message);
      Assert.assertTrue(
          message.contains("io.ray.test.CrossLanguageInvocationTest.throwException"), message);
      Assert.assertTrue(message.contains("java.lang.ArithmeticException: / by zero"), message);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testRaiseExceptionFromNestPython() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_nest_python_raise_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_nest_python_raise_exception"), message);
      Assert.assertTrue(message.contains("io.ray.runtime.task.TaskExecutor.execute"), message);
      Assert.assertTrue(message.contains("py_func_python_raise_exception"), message);
      Assert.assertTrue(message.contains("ZeroDivisionError: division by zero"), message);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testThrowExceptionFromNestJava() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_nest_java_throw_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_nest_java_throw_exception"), message);
      Assert.assertEquals(
          org.apache.commons.lang3.StringUtils.countMatches(
              message, "io.ray.runtime.exception.RayTaskException"),
          2);
      Assert.assertTrue(message.contains("py_func_java_throw_exception"), message);
      Assert.assertTrue(message.contains("java.lang.ArithmeticException: / by zero"), message);
      return;
    }
    Assert.fail();
  }

  public static Object[] pack(int i, String s, double f, Object[] o) {
    // This function will be called from test_cross_language_invocation.py
    return new Object[] {i, s, f, o};
  }

  public static Object returnInput(Object o) {
    return o;
  }

  public static boolean returnInputBoolean(boolean b) {
    return b;
  }

  public static int returnInputInt(int i) {
    return i;
  }

  public static double returnInputDouble(double d) {
    return d;
  }

  public static String returnInputString(String s) {
    return s;
  }

  public static int[] returnInputIntArray(int[] l) {
    return l;
  }

  public static byte[] callPythonActorHandle(PyActorHandle actor) {
    // This function will be called from test_cross_language_invocation.py
    ObjectRef<byte[]> res =
        actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "3".getBytes());
    return (byte[]) res.get();
  }

  @SuppressWarnings("ConstantOverflow")
  public static Object throwException() {
    return 1 / 0;
  }

  public static Object throwJavaException() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_java_throw_exception", Object.class))
            .remote();
    return res.get();
  }

  public static Object raisePythonException() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_python_raise_exception", Object.class))
            .remote();
    return res.get();
  }

  public static class TestActor {

    public TestActor(byte[] v) {
      value = v;
    }

    public byte[] concat(byte[] v) {
      byte[] c = new byte[value.length + v.length];
      System.arraycopy(value, 0, c, 0, value.length);
      System.arraycopy(v, 0, c, value.length, v.length);
      return c;
    }

    public byte[] value() {
      return value;
    }

    private byte[] value;
  }

  /// Cross language RuntimeEnv test.
  public void testSetSerializedRuntimeEnvForPythonActor() {
    Map<String, String> envVars = new HashMap<>();
    envVars.put("a", "b");
    Map<String, Object> runtimeEnvMap = new HashMap<>();
    runtimeEnvMap.put("env_vars", envVars);
    String jsonStr = new Gson().toJson(runtimeEnvMap);

    PyActorHandle actor =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "RuntimeEnvActor"))
            .setSerializedRuntimeEnv(jsonStr)
            .remote();

    ObjectRef<byte[]> res =
        actor.task(PyActorMethod.of("get_env_var", byte[].class), "a".getBytes()).remote();
    Assert.assertEquals(res.get(), "b".getBytes());
  }

  public void testJavaActorAffinityWithPythonActor() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    // python actor0 affinity with java actor1.
    ActorAffinitySchedulingStrategy schedulingStrategy0 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("java-language", false))
            .build();
    PyActorHandle actor0 =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes())
            .setSchedulingStrategy(schedulingStrategy0)
            .remote();
    ObjectRef<byte[]> res0 =
        actor0.task(PyActorMethod.of("increase", byte[].class), "0".getBytes()).remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          Assert.assertEquals(res0.get(3000), "1".getBytes());
        });

    // java actor1 affinity with python actor2.
    List<String> pythonValues =
        new ArrayList<String>() {
          {
            add("python");
          }
        };
    ActorAffinitySchedulingStrategy schedulingStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("language", pythonValues, false))
            .build();
    ActorHandle<JavaCounter> actor1 =
        Ray.actor(JavaCounter::new, 1)
            .setLabel("java-language", "yes")
            .setSchedulingStrategy(schedulingStrategy)
            .remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          actor1.task(JavaCounter::getValue).remote().get(3000);
        });

    // Nomal create python actor2.
    PyActorHandle actor2 =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes())
            .setLabel("language", "python")
            .remote();
    ObjectRef<byte[]> res =
        actor2.task(PyActorMethod.of("increase", byte[].class), "0".getBytes()).remote();
    Assert.assertEquals(res.get(3000), "1".getBytes());

    // Check if actor0 and actor1 is ready after actor2 is created.
    Assert.assertEquals(actor1.task(JavaCounter::getValue).remote().get(3000), Integer.valueOf(1));
    Assert.assertEquals(res0.get(3000), "1".getBytes());
  }

  public void testJavaActorAffinityWithCppActor() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    // Cpp actor0 depend on java actor1.
    ActorAffinitySchedulingStrategy schedulingStrategy0 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("java-language", false))
            .build();
    CppActorHandle actor0 =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setSchedulingStrategy(schedulingStrategy0)
            .remote();
    ObjectRef<byte[]> res0 =
        actor0.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          Assert.assertEquals(res0.get(3000), "C++ Worker".getBytes());
        });

    // Java actor1 depend on cpp actor2.
    List<String> cppValues =
        new ArrayList<String>() {
          {
            add("cpp");
          }
        };
    ActorAffinitySchedulingStrategy schedulingStrategy =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("language", cppValues, false))
            .build();
    ActorHandle<JavaCounter> actor1 =
        Ray.actor(JavaCounter::new, 1)
            .setLabel("java-language", "yes")
            .setSchedulingStrategy(schedulingStrategy)
            .remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          actor1.task(JavaCounter::getValue).remote().get(3000);
        });

    // Normal create actor2.
    CppActorHandle actor2 =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setLabel("language", "cpp")
            .remote();
    ObjectRef<byte[]> res2 =
        actor2.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.assertEquals(res2.get(3000), "C++ Worker".getBytes());

    // Check if actor0 and actor1 is ready after actor2 is created.
    Assert.assertEquals(actor1.task(JavaCounter::getValue).remote().get(3000), Integer.valueOf(1));
    Assert.assertEquals(res0.get(3000), "C++ Worker".getBytes());
  }

  public void testCrossActorAffinityWithCrossActor() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    // python actor affinity with python actor
    ActorAffinitySchedulingStrategy schedulingStrategy0 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("location", false))
            .build();
    PyActorHandle actor0 =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes())
            .setSchedulingStrategy(schedulingStrategy0)
            .remote();
    ObjectRef<byte[]> res0 =
        actor0.task(PyActorMethod.of("increase", byte[].class), "0".getBytes()).remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          Assert.assertEquals(res0.get(3000), "1".getBytes());
        });

    ActorAffinitySchedulingStrategy schedulingStrategy1 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("location", true))
            .build();
    PyActorHandle actor1 =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes())
            .setLabel("location", "dc-1")
            .setLabel("language", "python")
            .setSchedulingStrategy(schedulingStrategy1)
            .remote();
    ObjectRef<byte[]> res1 =
        actor1.task(PyActorMethod.of("increase", byte[].class), "0".getBytes()).remote();
    Assert.assertEquals(res1.get(3000), "1".getBytes());

    Assert.assertEquals(res0.get(3000), "1".getBytes());

    List<String> values =
        new ArrayList<String>() {
          {
            add("dc-2");
          }
        };
    ActorAffinitySchedulingStrategy schedulingStrategy2 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.notIn("location", values, true))
            .build();
    PyActorHandle actor2 =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes())
            .setSchedulingStrategy(schedulingStrategy2)
            .remote();
    ObjectRef<byte[]> res2 =
        actor2.task(PyActorMethod.of("increase", byte[].class), "0".getBytes()).remote();
    Assert.assertEquals(res2.get(3000), "1".getBytes());

    // cpp actor affinity with cpp actor
    ActorAffinitySchedulingStrategy schedulingStrategyCpp1 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("cpp-language", false))
            .build();
    CppActorHandle actorCpp1 =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setSchedulingStrategy(schedulingStrategyCpp1)
            .remote();
    ObjectRef<byte[]> resCpp1 =
        actorCpp1.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.expectThrows(
        RayTimeoutException.class,
        () -> {
          Assert.assertEquals(resCpp1.get(3000), "C++ Worker".getBytes());
        });

    ActorAffinitySchedulingStrategy schedulingStrategyCpp2 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.exists("cpp-language", true))
            .build();
    CppActorHandle actorCpp2 =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setLabel("cpp-language", "yes")
            .setSchedulingStrategy(schedulingStrategyCpp2)
            .remote();
    ObjectRef<byte[]> resCpp2 =
        actorCpp2.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.assertEquals(resCpp2.get(3000), "C++ Worker".getBytes());

    // cpp actorCpp3 affinity with python actor1
    List<String> cppValues =
        new ArrayList<String>() {
          {
            add("python");
          }
        };
    ActorAffinitySchedulingStrategy schedulingStrategyCpp3 =
        new ActorAffinitySchedulingStrategy.Builder()
            .addExpression(ActorAffinityMatchExpression.in("language", cppValues, false))
            .build();
    CppActorHandle actorCpp3 =
        Ray.actor(CppActorClass.of("RAY_FUNC(Counter::FactoryCreate)", "Counter"))
            .setSchedulingStrategy(schedulingStrategyCpp3)
            .remote();
    ObjectRef<byte[]> resCpp3 =
        actorCpp3.task(CppActorMethod.of("GetBytes", byte[].class), "C++ Worker").remote();
    Assert.assertEquals(resCpp3.get(3000), "C++ Worker".getBytes());
  }

  public void testPythonCallJavaActorAffinity() {
    if (!TestUtils.getRuntime().getRayConfig().gcsTaskSchedulingEnabled) {
      throw new SkipException("This actor affinity feature don't support raylet schedule mode.");
    }
    ObjectRef<byte[]> res =
        Ray.task(
                PyFunction.of(
                    PYTHON_MODULE, "py_func_call_java_actor_test_actor_affinity", byte[].class))
            .remote();
    Assert.assertEquals(res.get(), "OK".getBytes());
  }

  public void testGetCurrentNodeId() {
    ObjectRef<String> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_get_current_node_id_hex", String.class)).remote();
    Assert.assertEquals(res.get(), Ray.getRuntimeContext().getCurrentNodeId().toString());
  }
}
