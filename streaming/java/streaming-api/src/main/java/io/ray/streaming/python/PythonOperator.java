package io.ray.streaming.python;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.common.utils.CommonUtil;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.OperatorUtil;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;

/** Represents a {@link AbstractStreamOperator} that wraps python {@link PythonFunction}. */
@SuppressWarnings("unchecked")
public class PythonOperator extends AbstractStreamOperator {
  public static final String OPERATOR_MODULE = "raystreaming.operator";

  private final String moduleName;
  private final String className;
  private Schema inputSchema;

  public PythonOperator(String moduleName, String className) {
    super(null);
    this.moduleName = moduleName;
    this.className = className;
  }

  public PythonOperator(PythonFunction function) {
    super(function);
    this.moduleName = null;
    this.className = null;
  }

  @Override
  public int getId() {
    return super.getId();
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public PythonFunction getFunction() {
    return (PythonFunction) super.getFunction();
  }

  @Override
  public void open(List list, RuntimeContext runtimeContext) {
    throwUnsupportedException();
  }

  @Override
  public void saveCheckpoint(long checkpointId) {
    throwUnsupportedException();
  }

  @Override
  public void loadCheckpoint(long checkpointId) {
    throwUnsupportedException();
  }

  @Override
  public void close() {
    throwUnsupportedException();
  }

  <T> T throwUnsupportedException() {
    StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    Preconditions.checkState(trace.length >= 2);
    StackTraceElement traceElement = trace[2];
    String msg =
        String.format(
            "Method %s.%s shouldn't be called.",
            traceElement.getClassName(), traceElement.getMethodName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public OperatorInputType getOpType() {
    PythonFunction pythonFunction = getFunction();
    if (pythonFunction
        .getFunctionInterface()
        .equals(PythonFunction.FunctionInterface.SOURCE_FUNCTION)) {
      return OperatorInputType.SOURCE;
    } else {
      return OperatorInputType.ONE_INPUT;
    }
  }

  @Override
  public String getName() {
    if (!getClass().getSimpleName().equals(name)) {
      return "PythonOperator_" + getModuleName() + "." + getClassName();
    }
    return name;
  }

  // If we need to support two input stream for python, we need to abstract PythonOperator into
  // SourceOperator/OneInputOperator/TwoInputOperator
  public Schema getInputSchema() {
    return inputSchema;
  }

  public void setInputSchema(Schema inputSchema) {
    this.inputSchema = inputSchema;
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner =
        new StringJoiner(", ", PythonOperator.class.getSimpleName() + "[", "]");
    if (function != null) {
      stringJoiner.add("function='" + function + "'");
    } else {
      stringJoiner.add("moduleName='" + moduleName + "'").add("className='" + className + "'");
    }
    stringJoiner.add("name='" + getName() + "'");
    return stringJoiner.toString();
  }

  public static class ChainedPythonOperator extends PythonOperator {
    private final List<PythonOperator> operators;
    private final PythonOperator headOperator;
    private Set<PythonOperator> tailOperators;
    private final List<Map<String, String>> opConfigs;
    private final Map<String, Double> resource;
    private String chainedOperatorName;

    public ChainedPythonOperator(
        List<PythonOperator> operators,
        List<Map<String, String>> opConfigs,
        List<Map<String, Double>> resources) {
      super(null);
      Preconditions.checkArgument(!operators.isEmpty());
      this.operators = operators;
      this.opConfigs = opConfigs;
      this.opConfig = CommonUtil.chainConfigs(opConfigs);
      this.resource = CommonUtil.chainResources(resources);
      this.headOperator = operators.get(0);
      this.tailOperators =
          OperatorUtil.generateTailOperators(
                  this.operators.stream().map(x -> (StreamOperator) x).collect(Collectors.toList()))
              .stream()
              .map(x -> (PythonOperator) x)
              .collect(Collectors.toSet());
    }

    @Override
    public OperatorInputType getOpType() {
      return headOperator.getOpType();
    }

    @Override
    public Language getLanguage() {
      return Language.PYTHON;
    }

    public String getChainedOperatorName() {
      if (this.chainedOperatorName == null) {
        StringBuilder name = new StringBuilder();
        OperatorUtil.createName(
            operators.stream().map(x -> (StreamOperator) x).collect(Collectors.toList()),
            operators.get(0),
            name);
        this.chainedOperatorName = name.toString();
      }
      return this.chainedOperatorName;
    }

    @Override
    public String getName() {
      return operators.get(0).getName();
    }

    @Override
    public String toString() {
      return ImmutableList.of(this.operators).toString();
    }

    @Override
    public String getModuleName() {
      return throwUnsupportedException();
    }

    @Override
    public String getClassName() {
      return throwUnsupportedException();
    }

    @Override
    public PythonFunction getFunction() {
      return throwUnsupportedException();
    }

    public List<PythonOperator> getOperators() {
      return operators;
    }

    public PythonOperator getHeadOperator() {
      return headOperator;
    }

    public Set<PythonOperator> getTailOperators() {
      return tailOperators;
    }

    public List<Map<String, String>> getOpConfigs() {
      return opConfigs;
    }

    @Override
    public Schema getInputSchema() {
      return headOperator.getInputSchema();
    }

    @Override
    public boolean hasSchema() {
      if (tailOperators.size() > 1) {
        throw new UnsupportedOperationException(
            "This chainedOperator has at least two tail operators, can't call 'hasSchema'");
      }
      return tailOperators.iterator().next().hasSchema();
    }

    @Override
    public Schema getSchema() {
      if (tailOperators.size() > 1) {
        throw new UnsupportedOperationException(
            "This chainedOperator has at least two tail operators, can't call 'getSchema'");
      }
      return tailOperators.iterator().next().getSchema();
    }

    @Override
    public Map<String, Double> getResource() {
      return this.resource;
    }
  }
}
