import random
import copy
import threading
from collections import defaultdict
import logging
import time
from typing import Any, Dict, List, Optional

from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME, \
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.constants import BOTO_MAX_RETRIES, \
    BOTO_CREATE_MAX_RETRIES
# from ray.autoscaler._private.aws.config import bootstrap_aws
from ray.autoscaler._private.log_timer import LogTimer

# from ray.autoscaler._private.aws.utils import boto_exception_handler
from ray.autoscaler._private.cli_logger import cli_logger, cf

from ray.autoscaler._private.aliyun.utils import AcsClient
from ray.autoscaler._private.aliyun.config import PENDING, STOPPED, STARTING, STOPPING, RUNNING

logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1
STOPPING_NODE_DELAY = 1


class AliyunNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes",
                                                       True)
        self.acs = AcsClient(
            access_key=provider_config["access_key"],
            access_key_secret=provider_config["access_key_secret"],
            region=provider_config["region"],
            max_retries=BOTO_MAX_RETRIES,
        )

        self.acs_fail_fast = AcsClient(
            access_key=provider_config["access_key"],
            access_key_secret=provider_config["access_key_secret"],
            region=provider_config["region"],
            max_retries=1,
        )

        # Try availability zones round-robin, starting from random offset
        self.subnet_idx = random.randint(0, 100)

        # Tags that we believe to actually be on EC2.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        # Number of threads waiting for a batched tag update.
        self.batch_thread_count = 0
        self.batch_update_done = threading.Event()
        self.batch_update_done.set()
        self.ready_for_new_batch = threading.Event()
        self.ready_for_new_batch.set()
        self.tag_cache_lock = threading.Lock()
        self.count_lock = threading.Lock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        tags = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [self.cluster_name],
            },
        ]
        for k, v in tag_filters.items():
            tags.append({
                "Name": "tag:{}".format(k),
                "Values": [v],
            })

        instances = self.acs.describe_instances(tags=tags)
        non_terminated_instance = []
        for instance in instances:
            if instance.get('Status') != STOPPED and instance.get('Status') != STOPPING:
                non_terminated_instance.append(instance.get('InstanceId'))
                self.cached_nodes[instance.get('InstanceId')] = instance
        return non_terminated_instance

    def is_running(self, node_id: str) -> bool:
        instances = self.acs.describe_instances(instance_ids=[node_id])
        if instances is not None:
            instance = instances[0]
            return instance.get('Status') == "Running"
        cli_logger.error("Invalid node id: %s", node_id)
        return False

    def is_terminated(self, node_id: str) -> bool:
        instances = self.acs.describe_instances(instance_ids=[node_id])
        if instances is not None:
            assert len(instances) == 1
            instance = instances[0]
            return instance.get('Status') == "Stopped"
        cli_logger.error("Invalid node id: %s", node_id)
        return False

    def node_tags(self, node_id: str) -> Dict[str, str]:
        instances = self.acs.describe_instances(instance_ids=[node_id])
        if instances is not None:
            assert len(instances) == 1
            instance = instances[0]
            if instance.get('Tags') is not None:
                node_tags = dict()
                for tag in instance.get('Tags').get('Tag'):
                    node_tags[tag.get('TagKey')] = tag.get('TagValue')
                return node_tags
        return dict()

    def external_ip(self, node_id: str) -> str:
        instances = self.acs.describe_instances(instance_ids=[node_id])
        if instances is not None:
            assert len(instances)
            instance = instances[0]
            if instance.get('PublicIpAddress') is not None and instance.get('PublicIpAddress').get('IpAddress') is not None:
                if len(instance.get('PublicIpAddress').get('IpAddress')) > 0:
                    return instance.get('PublicIpAddress').get('IpAddress')[0]
            cli_logger.error("PublicIpAddress attribute is not exist")
            return None
        return None

    def internal_ip(self, node_id: str) -> str:
        instances = self.acs.describe_instances(instance_ids=[node_id])
        if instances is not None:
            assert len(instances)
            instance = instances[0]
            if instance.get('PrivateIpSets') is not None and instance.get('PrivateIpSets').get('PrivateIpSet') is not None and len(instance.get('PrivateIpSets').get('PrivateIpSet')) > 0:
                if instance.get('PrivateIpSets').get('PrivateIpSet')[0].get('Primary'):
                    return instance.get('PrivateIpSets').get('PrivateIpSet')[0].get('PrivateIpAddress')
            cli_logger.error("InnerIpAddress attribute is not exist")
            return ""
        return None

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        is_batching_thread = False
        with self.tag_cache_lock:
            if not self.tag_cache_pending:
                is_batching_thread = True
                # Wait for threads in the last batch to exit
                self.ready_for_new_batch.wait()
                self.ready_for_new_batch.clear()
                self.batch_update_done.clear()
            self.tag_cache_pending[node_id].update(tags)

        if is_batching_thread:
            time.sleep(TAG_BATCH_DELAY)
            with self.tag_cache_lock:
                self._update_node_tags()
                self.batch_update_done.set()

        with self.count_lock:
            self.batch_thread_count += 1
        self.batch_update_done.wait()

        with self.count_lock:
            self.batch_thread_count -= 1
            if self.batch_thread_count == 0:
                self.ready_for_new_batch.set()

    def _update_node_tags(self):
        batch_updates = defaultdict(list)

        for node_id, tags in self.tag_cache_pending.items():
            for x in tags.items():
                batch_updates[x].append(node_id)
            self.tag_cache[node_id] = tags

        self.tag_cache_pending = defaultdict(dict)

        self._create_tags(batch_updates)

    def _create_tags(self, batch_updates):

        for (k, v), node_ids in batch_updates.items():
            m = "Set tag {}={} on {}".format(k, v, node_ids)
            with LogTimer("AliyunNodeProvider: {}".format(m)):
                if k == TAG_RAY_NODE_NAME:
                    k = "Name"

                self.acs.tag_resource(node_ids, [{
                    "Key": k,
                    "Value": v
                }])

    def create_node(self, node_config: Dict[str, Any], tags: Dict[str, str],
                    count: int) -> Optional[Dict[str, Any]]:

        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["stopped", "stopping"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [self.cluster_name],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_NODE_KIND),
                "Values": [tags[TAG_RAY_NODE_KIND]],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_LAUNCH_CONFIG),
                "Values": [tags[TAG_RAY_LAUNCH_CONFIG]],
            },
        ]

        if TAG_RAY_USER_NODE_TYPE in tags:
            filters.append({
                "Name": "tag:{}".format(TAG_RAY_USER_NODE_TYPE),
                "Values": [tags[TAG_RAY_USER_NODE_TYPE]],
            })

        reuse_nodes = self.acs.describe_instances(tags=filters)[:count]
        # reuse_node_ids = [n.get('InstanceId') for n in reuse_nodes]
        # reused_nodes_dict = {n.get('InstanceId'): n for n in reuse_nodes}

        if reuse_nodes:
            with cli_logger.group("Stopping instances to reuse"):
                for node in reuse_nodes:
                    node_id = node.get('InstanceId')
                    self.tag_cache[node_id] = node.get('Tags')
                    self.set_node_tags(node_id, tags)
                    print('node_id:' + node_id)
                    print('node status:' + node.get('Status'))
                    if node.get('Status') == RUNNING:
                        self.acs.reboot_instance(node_id)

                    if node.get('Status') == STOPPED:
                        print('start instance')
                        self.acs.start_instance(node_id)
            count -= len(reuse_nodes)

        for k, v in tags.items():
            filters.append({
                "Name": "tag:{}".format(k),
                "Values": [v],
            })

        created_nodes_dict = dict()
        if count > 0:
            instance_id_sets = self.acs.run_instances(
                instance_type=node_config['InstanceType'],
                image_id=node_config['ImageId'],
                tags=filters,
                amount=count,
                vswitch_id=node_config['VSwitchId'],
                security_group_id=node_config['SecurityGroupId'],
                key_pair_name=node_config['KeyPairName']
            )
            instances = self.acs.describe_instances(instance_ids=instance_id_sets)

            if instances is not None:
                for instance in instances:
                    print(instance.get('Status'))
                    created_nodes_dict[instance.get('InstanceId')] = instance

        return created_nodes_dict

    def terminate_node(self, node_id: str) -> None:
        self.acs.stop_instance(node_id)

    def terminate_nodes(self, node_ids: List[str]) -> None:
        self.acs.stop_instances(node_ids)

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        # Node not in {pending, running} -- retry with a point query. This
        # usually means the node was recently preempted or terminated.
        matches = self.acs.describe_instances(instance_ids=[node_id])

        assert len(matches) == 1, "Invalid instance id {}".format(node_id)
        return matches[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)
