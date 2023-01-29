import time
import json
from libstreaming_kmonitor import StreamingKmonitor
json_str = {
    "tenant_name": "default",
    "service_name": "kmon",
    "system_metrics": "true",
    "sink_period": 1,
    "sink_queue_capacity": 1000,
    "sink_address": "11.166.67.226:4141",
    "golbal_tags": {
        "cluster": "kmon-dev",
        "app": "streaming"
    }
}

a = StreamingKmonitor()
a.start(json.dumps(json_str))

for i in range(0, 100):
    tag = {"a": "aa", "value": str(i % 10)}
    a.update_gauge("test.gauge", i, tag)
    a.update_counter("test.counter", 1, tag)
    for j in range(0, i * 3):
        a.update_qps("test.qps", 1, tag)
    time.sleep(1)
a.stop()
