package io.ray.streaming.common.config;

public interface ResourceConfig extends Config {

  String MASTER_MEM = "streaming.resource.master.memory.mb";
  String MASTER_PER_HUNDRED_PARALLELISM_MEM =
      "streaming.resource.master.per-hundred-parallelism.memory.gb";
  String WORKER_RESOURCE_STRICT_LIMIT_ENABLE = "streaming.resource.worker.strict-limit.enable";
  String WORKER_CPU = "streaming.resource.worker.cpu";
  String WORKER_MEM = "streaming.resource.worker.mem.mb";
  String WORKER_GPU = "streaming.resource.worker.gpu";
  String EXTRA_CPU = "streaming.resource.extra.cpu";
  String EXTRA_MEMORY_MB = "streaming.resource.extra.memory.mb";
  String WORKER_CPU_LIMIT = "streaming.resource.worker.cpu.limit";
  String WORKER_MEM_LIMIT = "streaming.resource.worker.mem.limit";
  String WORKER_GPU_LIMIT = "streaming.resource.worker.gpu.limit";
  String WORKER_CPU_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.cpu.recommendation.multiple.factor";
  String WORKER_MEM_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.mem.recommendation.multiple.factor";
  String WORKER_GPU_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.gpu.recommendation.multiple.factor";
  String OPERATOR_CUSTOM_RESOURCE = "streaming.resource.operator.custom.resource";
  String ENABLE_OPERATOR_PROPOSED_RESOURCE = "streaming.resource.operator.proposed.resource.enable";
  String OPERATOR_PROPOSED_RESOURCE = "streaming.resource.operator.proposed.resource";

  long JOB_MASTER_MAX_MEMORY_MB = 10000;

  /**
   * Memory for job master.
   *
   * @return job master's memory in mb
   */
  @DefaultValue(value = "3000")
  @Key(value = MASTER_MEM)
  int masterMemoryMb();

  /**
   * Gb memory for job master per hundred parallelism. e.g. total parallelism: 400 -> memory:
   * 400/100 * 1 = 4gb
   *
   * @return gb memory
   */
  @DefaultValue(value = "1")
  @Key(value = MASTER_PER_HUNDRED_PARALLELISM_MEM)
  int masterMemoryGbPerHundredParallelism();

  /**
   * If this value is false, scheduler will not care whether the worker has cpu or mem required.
   *
   * @return true or false
   */
  @DefaultValue(value = "true")
  @Key(value = WORKER_RESOURCE_STRICT_LIMIT_ENABLE)
  boolean isWorkerResourceStrictLimit();

  /**
   * Default cpu required for each worker. Unit: number. Warning: do not use float value(only
   * integer is supported) in cluster mode
   *
   * @return cpu number
   */
  @DefaultValue(value = "1")
  @Key(value = WORKER_CPU)
  double workerCpuRequired();

  /**
   * Default memory required for each worker unless jvm-opts is specified. Unit: mb. Warning: do not
   * use float value(only integer is supported) in cluster mode
   *
   * @return memory size
   */
  @DefaultValue(value = "1600")
  @Key(value = WORKER_MEM)
  double workerMemMbRequired();

  /**
   * Default gpu required for each worker. Unit: number. Warning: do not use float value(only
   * integer is supported) in cluster mode
   *
   * @return gpu number
   */
  @DefaultValue(value = "0")
  @Key(value = WORKER_GPU)
  double workerGpuRequired();

  /**
   * Extra cpu for job(including in total cpu number).
   *
   * @return cpu number
   */
  @DefaultValue(value = "0")
  @Key(value = EXTRA_CPU)
  double extraCpuNum();

  /**
   * Extra memory for job(including in total memory).
   *
   * @return memory in mb
   */
  @DefaultValue(value = "4000")
  @Key(value = EXTRA_MEMORY_MB)
  long extraMemoryMb();

  /**
   * User defined operator resource in job config. Must be json style, format: {opId, {resources'
   * json}} or {opName, {resources' json}}
   *
   * <p>e.g. {"10":{"CPU": "2"},"12":{"CPU": "3", "MEM": "4000"}} or {"10-MapOperator":{"CPU":
   * "2"},"12-SourceOperator":{"CPU": "3", "MEM": "4000"}} or {"10":{"CPU":
   * "2"},"12-SourceOperator":{"CPU": "3", "MEM": "4000"}}
   *
   * @return operator resource definition in json style
   */
  @DefaultValue(value = "{}")
  @Key(value = OPERATOR_CUSTOM_RESOURCE)
  String operatorCustomResource();

  /**
   * Whether to enable the default proposed operator resources.
   *
   * @return true or false
   */
  @DefaultValue(value = "true")
  @Key(value = ENABLE_OPERATOR_PROPOSED_RESOURCE)
  boolean enableProposedResource();

  /**
   * Default operator resource suggestion. Must be json style, format: {operator key name,
   * {resources' json}} or {operator key name, {resources' json}}
   *
   * <p>e.g. {"Source:{"CPU": "1", "MEM": "1200"},"Filter":{"CPU": "1", "MEM": "1200"},
   * "Map":{"CPU": "1", "MEM": "1200"},"Join":{"CPU": "1", "MEM": "2000"}}
   *
   * @return operator resource definition in json style
   */
  @DefaultValue(
      value =
          "{\"Source\":{\"MEM\": \"1200\"},"
              + "\"Join\":{\"CPU\": \"1\", \"MEM\": \"4000\"},"
              + "\"Window\":{\"CPU\": \"1\", \"MEM\": \"4000\"}}")
  @Key(value = OPERATOR_PROPOSED_RESOURCE)
  String operatorProposedResource();

  /**
   * Limit of the cpu resource for all kind of worker. Unit: number. format: minimum,maximum
   *
   * @return cpu limit
   */
  @DefaultValue(value = "0.1,8")
  @Separator(",")
  @Key(value = WORKER_CPU_LIMIT)
  float[] workerCpuLimit();

  /**
   * Limit of the memory resource for all kind of worker. Unit: mb. format: minimum,maximum
   *
   * @return memory limit
   */
  @DefaultValue(value = "1000,128000")
  @Separator(",")
  @Key(value = WORKER_MEM_LIMIT)
  float[] workerMemLimit();

  /**
   * Limit of the gpu resource for all kind of worker. Unit: number. format: minimum,maximum
   *
   * @return gpu limit
   */
  @DefaultValue(value = "0.1,4")
  @Separator(",")
  @Key(value = WORKER_GPU_LIMIT)
  float[] workerGpuLimit();

  /**
   * Recommendation multiple factor for Cpu resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return cpu factor
   */
  @DefaultValue(value = "2")
  @Key(value = WORKER_CPU_RECOMMENDATION_FACTOR)
  float workerCpuRecommendationFactor();

  /**
   * Recommendation multiple factor for Memory resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return memory factor
   */
  @DefaultValue(value = "1.3")
  @Key(value = WORKER_MEM_RECOMMENDATION_FACTOR)
  float workerMemRecommendationFactor();

  /**
   * Recommendation multiple factor for Gpu resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return gpu factor
   */
  @DefaultValue(value = "2")
  @Key(value = WORKER_GPU_RECOMMENDATION_FACTOR)
  float workerGpuRecommendationFactor();
}
