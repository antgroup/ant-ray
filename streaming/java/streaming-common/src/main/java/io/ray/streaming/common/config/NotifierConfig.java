package io.ray.streaming.common.config;

public interface NotifierConfig extends Config {
  String DINGTALK_NOTIFIER_ENABLE = "streaming.notifier.dingtalk.enable";
  String DINGTALK_NOTIFIER_TOKEN = "streaming.notifier.dingtalk.token";
  String DINGTALK_NOTIFIER_SEC = "streaming.notifier.dingtalk.sec";
  String DINGTALK_NOTIFIER_MOBILES = "streaming.notifier.dingtalk.mobiles";

  /**
   * Whether to enable dingtalk notifier or not. If enabled, adaption manager WILL NOT trigger
   * auto-scaling event but just send message to dingtalk.
   *
   * @return true if adaption manager enable dingtalk notifier, false if not enabled.
   */
  @DefaultValue(value = "false")
  @Key(value = DINGTALK_NOTIFIER_ENABLE)
  Boolean dingtalkNotifierEnable();

  /** @return Dingtalk notifier webhook token */
  @DefaultValue(value = "")
  @Key(value = DINGTALK_NOTIFIER_TOKEN)
  String dingtalkNotifierToken();

  /** @return Dingtalk notifier webhook sec */
  @DefaultValue(value = "")
  @Key(value = DINGTALK_NOTIFIER_SEC)
  String dingtalkNotifierSec();

  /** @return Dingtalk notifier @mobiles */
  @DefaultValue(value = "")
  @Key(value = DINGTALK_NOTIFIER_MOBILES)
  String dingtalkNotifierMobiles();
}
