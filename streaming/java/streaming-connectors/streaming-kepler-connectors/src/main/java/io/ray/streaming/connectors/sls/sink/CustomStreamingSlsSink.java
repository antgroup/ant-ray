package io.ray.streaming.connectors.sls.sink;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfigs;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.base.Preconditions;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.RichFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.connectors.sls.config.SlsProducerConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomStreamingSlsSink<T> implements SinkFunction<T>, RichFunction {

  private static final Logger LOG = LoggerFactory.getLogger(CustomStreamingSlsSink.class);

  // Config key.
  private static final String ENDPOINT_CONFIG_KEY = "streaming.output.sls.endpoint";
  private static final String PROJECT_CONFIG_KEY = "streaming.output.sls.project";
  private static final String LOGSTORE_CONFIG_KEY = "streaming.output.sls.logstore";
  private static final String ACCESSID_CONFIG_KEY = "streaming.output.sls.accessid";
  private static final String ACCESSKEY_CONFIG_KEY = "streaming.output.sls.accesskey";

  // Config value.
  private String ENDPOINT_VALUE;
  private String PROJECT_VALUE;
  private String LOGSTORE_VALUE;
  private String ACCESSID_VALUE;
  private String ACCESSKEY_VALUE;

  // Producer.
  Producer slsProducer;

  @Override
  public void open(RuntimeContext runtimeContext) {
    ENDPOINT_VALUE = runtimeContext.getJobConfig().get(ENDPOINT_CONFIG_KEY);
    PROJECT_VALUE = runtimeContext.getJobConfig().get(PROJECT_CONFIG_KEY);
    LOGSTORE_VALUE = runtimeContext.getJobConfig().get(LOGSTORE_CONFIG_KEY);
    ACCESSID_VALUE = runtimeContext.getJobConfig().get(ACCESSID_CONFIG_KEY);
    ACCESSKEY_VALUE = runtimeContext.getJobConfig().get(ACCESSKEY_CONFIG_KEY);
    LOG.info(
        "Init sls sink with config: endpoint: {}, project: {}, logstore: {}, accessid: {}, accesskey: {}",
        ENDPOINT_VALUE,
        PROJECT_VALUE,
        LOGSTORE_VALUE,
        ACCESSID_VALUE,
        ACCESSKEY_VALUE);

    Preconditions.checkState(
        ENDPOINT_VALUE != null
            && PROJECT_VALUE != null
            && LOGSTORE_VALUE != null
            && ACCESSID_VALUE != null
            && ACCESSKEY_VALUE != null);
    try {
      // 1.
      ProjectConfig projectConfig =
          new ProjectConfig(PROJECT_VALUE, ENDPOINT_VALUE, ACCESSID_VALUE, ACCESSKEY_VALUE);
      // 2.
      ProjectConfigs projectConfigs = new ProjectConfigs();
      projectConfigs.put(projectConfig);
      ProducerConfig producerConfig = new ProducerConfig(projectConfigs);
      // 3.
      SlsProducerConfigGenerator.generateProducerFromJobConfig(
          producerConfig, runtimeContext.getJobConfig());
      // 4.
      slsProducer = new LogProducer(producerConfig);
    } catch (Exception e) {
      LOG.error("Sls producer init failed!");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      slsProducer.close();
    } catch (Exception ignore) {
    }
  }

  @Override
  public void sink(T value) {
    try {
      slsProducer.send(PROJECT_VALUE, LOGSTORE_VALUE, generateLogItem(value));
    } catch (Exception ignore) {
    }
  }

  public LogItem generateLogItem(T value) {
    LogItem logItem = new LogItem();
    logItem.PushBack("content", value.toString());
    LOG.info("Send sls data: {}.", value);
    return logItem;
  }
}
