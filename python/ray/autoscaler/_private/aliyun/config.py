import logging
import os

from ray.autoscaler._private.aliyun.utils import AcsClient

# instance status
PENDING = "Pending"
RUNNING = "Running"
STARTING = "Starting"
STOPPING = "Stopping"
STOPPED = "Stopped"


def bootstrap_aliyun(config):
    # print(config["provider"])
    # create vpc
    _get_or_create_vpc(config)
    # create security group id
    _get_or_create_security_group(config)
    # create vswitch
    _get_or_create_vswitch(config)
    # create key pair
    _get_or_import_key_pair(config)
    # print(config["provider"])
    return config

def _client(config):
    return AcsClient(
        access_key=config["provider"]["access_key"],
        access_key_secret=config["provider"]["access_key_secret"],
        region=config["provider"]["region"],
        max_retries=1,
    )

def _get_or_create_security_group(config):
    cli = _client(config)
    security_groups = cli.describe_security_groups(vpc_id=config["provider"]["vpc_id"])
    if security_groups is not None and len(security_groups) > 0:
        config["provider"]["security_group_id"] = security_groups[0]['SecurityGroupId']
        return config

    security_group_id = cli.create_security_group(vpc_id=config["provider"]["VpcId"])
    config["provider"]["security_group_id"] = security_group_id
    return

def _get_or_create_vpc(config):
    cli = _client(config)
    vpcs = cli.describe_vpcs()
    if vpcs is not None and len(vpcs) > 0:
        config["provider"]["vpc_id"] = vpcs[0].get('VpcId')
        return

    vpc_id = cli.create_vpc()
    if vpc_id is not None:
        config["provider"]["vpc_id"] = vpc_id


def _get_or_create_vswitch(config):
    cli = _client(config)
    vswitches = cli.describe_v_switches(vpc_id=config["provider"]["vpc_id"])
    if vswitches is not None and len(vswitches) > 0:
        config["provider"]["v_switch_id"] = vswitches[0].get('VSwitchId')
        return

    v_switch_id = cli.create_v_switch(
        vpc_id=config["provider"]["vpc_id"],
        zone_id=config["provider"]["zone_id"],
        cidr_block=config["provider"]["cidr_block"]
    )

    if v_switch_id is not None:
        config["provider"]["v_switch_id"] = v_switch_id

def _get_or_import_key_pair(config):
    cli = _client(config)
    name = config["provider"].get('key_name', None)

    if name is None:
        logging.error("Please provide key_pair attribute")
        return

    keypairs = cli.describe_key_pairs(key_pair_name=name)
    if keypairs is not None and len(keypairs) == 1:
        return

    with open(os.path.expanduser('~/.ssh/id_rsa.pub')) as f:
        public_key = f.readline().strip('\n')
        print(public_key)
        cli.import_key_pair(key_pair_name=name, public_key_body=public_key)





