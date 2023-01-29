package io.ray.streaming.connectors.sls.source;

import com.alipay.kepler.common.config.ConfigKey;
import com.alipay.kepler.plugin.api.input.RawMessage;
import com.alipay.kepler.plugin.source.KeplerSource;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A customer source that support cache to make the checkpoint barrier easily to downstream. */
public class CustomerStreamingKeplerSlsSource extends KeplerSource {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerStreamingKeplerSlsSource.class);

  // a in-memory cache.
  private List<LogContent> consumeCache = new ArrayList<>();

  @Override
  public void init(int parallel, int index) {
    getRuntimeContext()
        .getOpConfig()
        .put(ConfigKey.INPUT_DATA_FETCHER_CLASS, "com.alipay.kepler.plugin.input.AsyncDataFetcher");
    getRuntimeContext()
        .getOpConfig()
        .put(ConfigKey.INPUT_CLASS, "com.alipay.kepler.plugins.input.sls.ng.SlsReader");
    super.init(parallel, index);

    LOG.info("Init KeplerSource, parallel={}, index={}.", parallel, index);
  }

  @Override
  public void fetch(long batchId, SourceContext trueContext) throws Exception {
    // Construct a new `SourceContext` to implements our customer logic.
    SlsSourceContext context = new SlsSourceContext(trueContext);
    // 3. Consume from the local cache is not empty.
    if (!this.consumeCache.isEmpty()) {
      LOG.info("The cache is not empty, will consume from the in memory cache.");
      LogContent content = this.consumeCache.get(0);
      // NOTE: When we collect data from cache, we need use the true context that comes from ray
      // streaming!
      trueContext.collect(content.GetValue());
      this.consumeCache.remove(0);
    } else {
      LOG.info("The cache is empty, will pull new data from sls!");
      // In this function, we will fill up the cache.
      super.fetch(batchId, context);
    }
  }

  private class SlsSourceContext implements SourceContext {
    SourceContext ctx;

    public SlsSourceContext(SourceContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public void collect(Object element) throws Exception {
      // 1. Instead of pushing the data to downstream directly, we put the data into the cache when
      // collecting!
      if (element instanceof RawMessage) {
        RawMessage msg = (RawMessage) element;
        List<LogItem> items = (List<LogItem>) msg.getContent();
        for (LogItem item : items) {
          // Put data to cache.
          consumeCache.addAll(item.GetLogContents());
        }
        LOG.info("Put {} of new data that come from sls to cache.", consumeCache.size());
      }

      // 2. Try to push one data to downstream, the cache probably still has extra data after this!
      if (!consumeCache.isEmpty()) {
        LogContent content = consumeCache.get(0);
        this.ctx.collect(content.GetValue());
        // NOTE: Remove from cache!
        consumeCache.remove(0);
      }
    }
  }
}
