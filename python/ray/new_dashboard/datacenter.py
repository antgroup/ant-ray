import os
import logging
import itertools
import ray
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.memory_utils as memory_utils
from ray.new_dashboard.modules.actor.actor_utils import \
    actor_classname_from_task_spec
from ray.new_dashboard.utils import \
    LimitedCapacityDict, Dict, Signal, async_loop_forever

from ray.new_dashboard.modules.job.job_consts import \
    DEFAULT_INACTIVE_JOBS_CAPACITY, DEFAULT_DEAD_NODES_CAPACITY

logger = logging.getLogger(__name__)


class GlobalSignals:
    gcs_reconnected = Signal(dashboard_consts.SIGNAL_GCS_RECONNECTED)
    node_info_fetched = Signal(dashboard_consts.SIGNAL_NODE_INFO_FETCHED)
    node_summary_fetched = Signal(dashboard_consts.SIGNAL_NODE_SUMMARY_FETCHED)
    job_info_fetched = Signal(dashboard_consts.SIGNAL_JOB_INFO_FETCHED)
    worker_info_fetched = Signal(dashboard_consts.SIGNAL_WORKER_INFO_FETCHED)
    dashboard_restarted = Signal(dashboard_consts.SIGNAL_DASHBOARD_RESTARTED)


def get_inactive_jobs_capacity():
    return int(
        os.environ.get("DASHBOARD_INACTIVE_JOBS_CAPACITY",
                       DEFAULT_INACTIVE_JOBS_CAPACITY))


def get_dead_nodes_capacity():
    return int(
        os.environ.get("DASHBOARD_DEAD_NODES_CAPACITY",
                       DEFAULT_DEAD_NODES_CAPACITY))


class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    dead_actors = Dict()
    # {job id hex(str): job table data(dict of JobTableData in gcs.proto)}
    jobs = Dict()
    # {job id hex(str): job table data(dict of JobTableData in gcs.proto)}
    inactive_jobs = LimitedCapacityDict(get_inactive_jobs_capacity())
    # {node id hex(str): {
    #   DASHBOARD_AGENT_HTTP_PORT: http port(int),
    #   DASHBOARD_AGENT_GRPC_PORT: grpc port(int)}}
    agents = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    dead_nodes = LimitedCapacityDict(get_dead_nodes_capacity())
    # {node id hex(str): ip address(str)}
    node_id_to_ip = Dict()
    # {node id hex(str): hostname(str)}
    node_id_to_hostname = Dict()
    # {node id hex(str): worker list}
    node_workers = Dict()
    # {node id hex(str): {actor id hex(str): actor table data}}
    node_actors = Dict()
    # {job id hex(str): worker list}
    job_workers = Dict()
    # {job id hex(str): {actor id hex(str): actor table data}}
    job_actors = Dict()
    # {worker id(str): core worker stats}
    core_worker_stats = Dict()
    # {job id hex(str): event list}
    events = Dict()
    # {job id hex(str): event list with specific label prefix}
    job_pipeline_events = Dict()
    # {job id hex(str): {worker id hex(str): {index(int): [event, ...]}}}
    job_worker_pipeline_events = Dict()
    # {job id hex(str): {actor id hex(str): {index(int): [event, ...]}}}
    job_actor_pipeline_events = Dict()
    # {worker id(str): {index(int): [event, ...]}}
    worker_pipeline_events = Dict()
    # {actor id hex(str): {index(int): [event, ...]}}
    actor_pipeline_events = Dict()
    # {node ip (str): log entries by pid
    # (dict from pid to list of latest log entries)}
    ip_and_pid_to_logs = Dict()
    # {node ip (str): error entries by pid
    # (dict from pid to list of latest err entries)}
    ip_and_pid_to_errors = Dict()
    # {
    #   timestamp(str): timestamp(int),
    #   resources(str): {
    #       resource name(str): {total: double, available: double}
    #   }
    # }
    cluster_resources = Dict()
    # {
    #   timestamp(str): timestamp(int),
    #   resources(str): {
    #      job id(str): {
    #       job id(str): job id,
    #       acquired resources(str): {
    #         resource name(str): resource value(double)
    #       },
    #       nodes(str): [
    #         {
    #           node id(str): node id,
    #           job resources(str): {
    #             acquired resources(str): {
    #               resource name(str): resource value(double)
    #             }
    #           },
    #           process resources(str): [
    #             pid(str): process id,
    #             acquired resources(str): {
    #               resource name(str): resource value(double)
    #             }
    #           ],
    #           node resources(str): {
    #             resource name(str): {
    #               total: double, available:double
    #             }
    #           }
    #         }
    #       ]
    #     }
    #   }
    # }
    job_resources = Dict()
    # {
    #   timestamp(str): timestamp(int),
    #   resources(str): {
    #     node id(str): {
    #       node id(str): node id,
    #       resources(str): {
    #         resource name(str): {
    #           total: double, available: double
    #         },
    #         jobs(str): [
    #           {
    #             job id(str): job id,
    #             job resource(str): {
    #               acquired resources(str): {
    #                 resource name(str): resource value(double)
    #               }
    #             },
    #             process resources(str): [
    #               pid(str): process id,
    #               acquired resources(str): {
    #                 resource name(str): resource value(double)
    #               }
    #             ]
    #           }
    #         ]
    #       }
    #     }
    #   }
    # }
    node_resources = Dict()
    # map from node name to host name.
    nodename_to_hostname = Dict()
    # map from host name to node name.
    hostname_to_nodename = Dict()


class DataOrganizer:
    @classmethod
    @async_loop_forever(dashboard_consts.ORGANIZE_DATA_INTERVAL_SECONDS)
    async def organize(cls):
        logger.info("Organize data.")
        job_workers = {}
        node_workers = {}
        core_worker_stats = {}
        # await inside for loop, so we create a copy of keys().
        for node_id in itertools.chain(DataSource.nodes.keys(),
                                       DataSource.dead_nodes.keys()):
            workers = await cls._get_node_workers(node_id)
            node_workers[node_id] = workers
            for worker in workers:
                for stats in worker.get("coreWorkerStats", []):
                    worker_id = stats["workerId"]
                    core_worker_stats[worker_id] = stats
            # Skip adding dead nodes' workers to job workers
            if node_id in DataSource.dead_nodes:
                continue
            for worker in workers:
                job_id = worker["jobId"]
                job_workers.setdefault(job_id, []).append(worker)
        DataSource.job_workers.reset(job_workers)
        DataSource.node_workers.reset(node_workers)
        DataSource.core_worker_stats.reset(core_worker_stats)

    @classmethod
    @async_loop_forever(dashboard_consts.LOG_INFO_INTERVAL_SECONDS)
    async def log_info(cls):
        logger.info("DataSource actors: %s, dead actors: %s",
                    len(DataSource.actors), len(DataSource.dead_actors))

    @classmethod
    async def _get_node_workers(cls, node_id):
        workers = []
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        node_stats = DataSource.node_stats.get(node_id, {})
        node = (DataSource.nodes.get(node_id, {})
                or DataSource.dead_nodes.get(node_id, {}))

        def _guess_language_from_cmdline(w):
            """ Guess the worker language from cmdline in case
            the coreWorkerStats is empty.
            """
            try:
                if "java" in w["cmdline"][0].lower():
                    return "JAVA"
                elif "python" in w["cmdline"][0].lower():
                    return "PYTHON"
            except Exception:
                pass
            return "CPP"

        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        pid_to_core_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}
        for stats in node_stats.get("coreWorkersStats", []):
            pid = stats["pid"]
            pid_to_core_worker_stats.setdefault(pid, []).append(stats)
            pid_to_language[pid] = stats["language"]
            pid_to_job_id[pid] = stats["jobId"]
        for worker in node_physical_stats.get("workers", []):
            worker = dict(worker)
            pid = worker["pid"]
            worker["coreWorkerStats"] = pid_to_core_worker_stats.get(pid, [])
            worker["language"] = (pid_to_language.get(pid)
                                  or _guess_language_from_cmdline(worker))
            worker["jobId"] = pid_to_job_id.get(pid, ray.JobID.nil().hex())
            # raylet info is more reliable than node physical stats.
            worker["ip"] = node.get("nodeManagerAddress", "unknown")
            worker["hostname"] = node.get("nodeManagerHostname", "unknown")

            await GlobalSignals.worker_info_fetched.send(node_id, worker)

            workers.append(worker)
        return workers

    @classmethod
    async def get_node_info(cls, node_id):
        node_physical_stats = dict(
            DataSource.node_physical_stats.get(node_id, {}))
        node_stats = dict(DataSource.node_stats.get(node_id, {}))
        node = (DataSource.nodes.get(node_id, {})
                or DataSource.dead_nodes.get(node_id, {}))
        node_ip = DataSource.node_id_to_ip.get(node_id)
        # Merge node log count information into the payload
        log_info = DataSource.ip_and_pid_to_logs.get(node_ip, {})
        node_log_count = 0
        for entries in log_info.values():
            node_log_count += len(entries)
        error_info = DataSource.ip_and_pid_to_errors.get(node_ip, {})
        node_err_count = 0
        for entries in error_info.values():
            node_err_count += len(entries)

        timestamp = {
            "node_physical_stats": node_physical_stats.get("timestamp", 0),
            "node_stats": node_stats.get("timestamp", 0),
        }

        node_stats.pop("coreWorkersStats", None)

        ray_stats = cls._extract_view_data(
            node_stats.get("viewData", []),
            {"object_store_used_memory", "object_store_available_memory"})

        node_info = node_physical_stats
        # Merge node stats to node physical stats under raylet
        node_info["raylet"] = node_stats
        node_info["raylet"].update(ray_stats)

        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        # Remove timestamp raylet data
        node_info["raylet"].pop("timestamp", None)
        # Merge actors to node physical stats
        node_info["actors"] = DataSource.node_actors.get(node_id, {})
        # Update workers to node physical stats
        node_info["workers"] = DataSource.node_workers.get(node_id, [])
        # Update timestamp
        node_info["timestamp"] = timestamp

        node_info["logCount"] = node_log_count
        node_info["errorCount"] = node_err_count
        await GlobalSignals.node_info_fetched.send(node_info)

        return node_info

    @classmethod
    async def get_node_summary(cls, node_id):
        node_physical_stats = dict(
            DataSource.node_physical_stats.get(node_id, {}))
        node_stats = dict(DataSource.node_stats.get(node_id, {}))
        node = (DataSource.nodes.get(node_id, {})
                or DataSource.dead_nodes.get(node_id, {}))

        timestamp = {
            "node_physical_stats": node_physical_stats.get("timestamp", 0),
            "node_stats": node_stats.get("timestamp", 0),
        }

        node_physical_stats.pop("workers", None)

        node_summary = node_physical_stats
        # Keep necessary information for front end
        node_summary.pop("cmdline", None)
        node_summary["raylet"] = \
            {"state": node["state"], "nodeId": node["nodeId"],
                "nodeManagerAddress": node["nodeManagerAddress"],
                "nodeManagerHostname": node["nodeManagerHostname"],
                "startTime": node["startTime"]}
        # Update timestamp
        node_summary["timestamp"] = timestamp

        await GlobalSignals.node_summary_fetched.send(node_summary)

        return node_summary

    @classmethod
    async def get_nodes_summary(cls, exclude_inactive=False):
        node_ids = DataSource.nodes.keys() if exclude_inactive else \
            itertools.chain(DataSource.nodes.keys(),
                            DataSource.dead_nodes.keys())
        return [
            await DataOrganizer.get_node_summary(node_id)
            for node_id in node_ids
        ]

    @classmethod
    async def get_nodes_details(cls, exclude_inactive=False):
        node_ids = DataSource.nodes.keys() if exclude_inactive else \
            itertools.chain(DataSource.nodes.keys(),
                            DataSource.dead_nodes.keys())
        return [
            await DataOrganizer.get_node_info(node_id) for node_id in node_ids
        ]

    @classmethod
    async def get_all_actors(cls):
        return {
            actor_id: await cls._get_actor(actor)
            for actor_id, actor in itertools.chain(
                DataSource.actors.items(), DataSource.dead_actors.items())
        }

    @staticmethod
    async def _get_actor(actor):
        actor = dict(actor)
        worker_id = actor["address"]["workerId"]
        core_worker_stats = DataSource.core_worker_stats.get(worker_id, {})
        actor_constructor = core_worker_stats.get("actorTitle",
                                                  "Unknown actor constructor")
        actor["actorConstructor"] = actor_constructor
        actor.update(core_worker_stats)

        # TODO(fyrestone): remove this, give a link from actor
        # info to worker info in front-end.
        node_id = actor["address"]["rayletId"]
        pid = core_worker_stats.get("pid")
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        actor_process_stats = None
        actor_process_gpu_stats = []
        if pid:
            for process_stats in node_physical_stats.get("workers", []):
                if process_stats["pid"] == pid:
                    actor_process_stats = process_stats
                    break

            for gpu_stats in node_physical_stats.get("gpus", []):
                for process in gpu_stats.get("processes", []):
                    if process["pid"] == pid:
                        actor_process_gpu_stats.append(gpu_stats)
                        break

        actor["gpus"] = actor_process_gpu_stats
        actor["processStats"] = actor_process_stats
        return actor

    @classmethod
    async def get_actor_creation_tasks(cls):
        infeasible_tasks = sum(
            (list(node_stats.get("infeasibleTasks", []))
             for node_stats in DataSource.node_stats.values()), [])
        new_infeasible_tasks = []
        for task in infeasible_tasks:
            task = dict(task)
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "INFEASIBLE"
            new_infeasible_tasks.append(task)

        resource_pending_tasks = sum(
            (list(data.get("readyTasks", []))
             for data in DataSource.node_stats.values()), [])
        new_resource_pending_tasks = []
        for task in resource_pending_tasks:
            task = dict(task)
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "PENDING_RESOURCES"
            new_resource_pending_tasks.append(task)

        results = {
            task["actorCreationTaskSpec"]["actorId"]: task
            for task in new_resource_pending_tasks + new_infeasible_tasks
        }
        return results

    @classmethod
    async def get_memory_table(cls,
                               sort_by=memory_utils.SortingType.OBJECT_SIZE,
                               group_by=memory_utils.GroupByType.STACK_TRACE):
        all_worker_stats = []
        for node_stats in DataSource.node_stats.values():
            all_worker_stats.extend(node_stats.get("coreWorkersStats", []))
        memory_information = memory_utils.construct_memory_table(
            all_worker_stats, group_by=group_by, sort_by=sort_by)
        return memory_information

    @staticmethod
    def _extract_view_data(views, data_keys):
        view_data = {}
        for view in views:
            view_name = view["viewName"]
            if view_name in data_keys:
                if not view.get("measures"):
                    view_data[view_name] = 0
                    continue
                measure = view["measures"][0]
                if "doubleValue" in measure:
                    measure_value = measure["doubleValue"]
                elif "intValue" in measure:
                    measure_value = measure["intValue"]
                else:
                    measure_value = 0
                view_data[view_name] = measure_value

        return view_data
