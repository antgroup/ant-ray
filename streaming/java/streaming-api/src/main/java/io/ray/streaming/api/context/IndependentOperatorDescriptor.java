package io.ray.streaming.api.context;

import com.google.common.base.MoreObjects;
import com.google.gson.Gson;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.BaseIndependentOperator;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** To describe {@link BaseIndependentOperator}. */
public class IndependentOperatorDescriptor implements Serializable {

  private final String className;
  private final String moduleName;
  private final Language language;
  private String independentOperatorName;

  private int parallelism;
  private Map<String, Double> resource;
  private Map<String, String> config;

  /** Will be scheduled after DAG's scheduling if true. */
  private boolean isLazyScheduling;

  public IndependentOperatorDescriptor(String className) {
    this(className, "", Language.JAVA);
  }

  public IndependentOperatorDescriptor(String className, String moduleName) {
    this(className, moduleName, StringUtils.isEmpty(moduleName) ? Language.JAVA : Language.PYTHON);
  }

  public IndependentOperatorDescriptor(String className, String moduleName, Language language) {
    this(className, moduleName, language, 1, "", new HashMap<>(), new HashMap<>(), false);
  }

  public IndependentOperatorDescriptor(
      String className,
      String moduleName,
      Language language,
      int parallelism,
      String independentOperatorName,
      Map<String, Double> resource,
      Map<String, String> config,
      boolean isLazyScheduling) {
    this.className = className;
    this.moduleName = moduleName;
    this.language = language;
    this.parallelism = parallelism;
    this.independentOperatorName = independentOperatorName;
    this.resource = resource;
    this.config = config;
    this.isLazyScheduling = isLazyScheduling;
  }

  public String getClassName() {
    return className;
  }

  public String getModuleName() {
    return moduleName;
  }

  public Language getLanguage() {
    return language;
  }

  public int getParallelism() {
    return parallelism;
  }

  public IndependentOperatorDescriptor setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public Map<String, Double> getResource() {
    return resource;
  }

  public IndependentOperatorDescriptor setResource(Map<String, Double> resource) {
    this.resource = resource;
    return this;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public IndependentOperatorDescriptor setConfig(Map<String, String> config) {
    this.config = config;
    return this;
  }

  public boolean isLazyScheduling() {
    return isLazyScheduling;
  }

  public IndependentOperatorDescriptor setLazyScheduling() {
    isLazyScheduling = true;
    return this;
  }

  public IndependentOperatorDescriptor setLazyScheduling(boolean isLazyScheduling) {
    this.isLazyScheduling = isLazyScheduling;
    return this;
  }

  public IndependentOperatorDescriptor setName(String name) {
    this.independentOperatorName = name;
    return this;
  }

  public boolean isJavaType() {
    return language == Language.JAVA;
  }

  public boolean isPythonType() {
    return language == Language.PYTHON;
  }

  public String asJson() {
    return new Gson().toJson(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndependentOperatorDescriptor that = (IndependentOperatorDescriptor) o;
    return className.equals(that.className)
        && moduleName.equals(that.moduleName)
        && language == that.language;
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, moduleName, language);
  }

  private String getSimpleDesc() {
    if (isJavaType()) {
      return className;
    } else {
      return className + "-" + moduleName;
    }
  }

  public String getName() {
    if (StringUtils.isEmpty(independentOperatorName)) {
      return getSimpleDesc();
    }
    return independentOperatorName;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("className", className)
        .add("moduleName", moduleName)
        .add("language", language)
        .add("parallelism", parallelism)
        .add("independentOperatorName", independentOperatorName)
        .add("resource", resource)
        .add("config", config)
        .add("isLazyScheduling", isLazyScheduling)
        .toString();
  }
}
