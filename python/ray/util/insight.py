import ray
from collections import defaultdict

@ray.remote
class InsightMonitor:
    def __init__(self):
        # {job_id: {caller_class.caller_func -> callee_class.callee_func: count}}
        self.call_graph = defaultdict(lambda: defaultdict(int))
        # Maps to track unique actors and methods per job
        self.actors = defaultdict(set)
        self.actor_id_map = defaultdict(dict)  # {job_id: {actor_class: actor_id}}
        self.methods = defaultdict(dict)  # {job_id: {class.method: {id: unique_id, actorId: actor_id}}}
        self.functions = defaultdict(set)
        self.function_id_map = defaultdict(dict)  # {job_id: {function_name: function_id}}
        self.actor_counter = defaultdict(int)
        self.method_counter = defaultdict(int)
        self.function_counter = defaultdict(int)

    def emit_call_record(self, call_record):
        job_id = call_record["job_id"]
        caller_class = call_record["caller_class"]
        caller_func = call_record["caller_func"]
        callee_class = call_record["callee_class"]
        callee_func = call_record["callee_func"]
        call_times = call_record.get("call_times", 1)

        # Create caller and callee identifiers
        caller_id = f"{caller_class}.{caller_func}" if caller_class else caller_func
        callee_id = f"{callee_class}.{callee_func}" if callee_class else callee_func

        # Update call graph
        self.call_graph[job_id][f"{caller_id}->{callee_id}"] += call_times

        # Track actors and methods
        if caller_class:
            self.actors[job_id].add(caller_class)
            if caller_class not in self.actor_id_map[job_id]:
                self.actor_id_map[job_id][caller_class] = caller_class.split(":")[1]

            if caller_id not in self.methods[job_id]:
                self.method_counter[job_id] += 1
                self.methods[job_id][caller_id] = {
                    "id": f"method{self.method_counter[job_id]}",
                    "actorId": self.actor_id_map[job_id][caller_class],
                    "name": caller_func,
                    "class": caller_class
                }
        else:
            self.functions[job_id].add(caller_func)
            if caller_func not in self.function_id_map[job_id]:
                if caller_func == "main":
                    self.function_id_map[job_id][caller_func] = "main"
                else:
                    self.function_counter[job_id] += 1
                    self.function_id_map[job_id][caller_func] = f"function{self.function_counter[job_id]}"

        if callee_class:
            self.actors[job_id].add(callee_class)
            if callee_class not in self.actor_id_map[job_id]:
                self.actor_id_map[job_id][callee_class] = callee_class.split(":")[1]

            if callee_id not in self.methods[job_id]:
                self.method_counter[job_id] += 1
                self.methods[job_id][callee_id] = {
                    "id": f"method{self.method_counter[job_id]}",
                    "actorId": self.actor_id_map[job_id][callee_class],
                    "name": callee_func,
                    "class": callee_class
                }
        else:
            self.functions[job_id].add(callee_func)
            if callee_func not in self.function_id_map[job_id]:
                if callee_func == "main":
                    self.function_id_map[job_id][callee_func] = "main"
                else:
                    self.function_counter[job_id] += 1
                    self.function_id_map[job_id][callee_func] = f"function{self.function_counter[job_id]}"

    def get_call_graph_data(self, job_id):
        """Return the call graph data for a specific job."""
        graph_data = {
            "actors": [],
            "methods": [],
            "functions": [],
            "callFlows": [],
            "dataFlows": []
        }

        # Add actors
        for actor_class, actor_id in self.actor_id_map.get(job_id, {}).items():
            graph_data["actors"].append({
                "id": actor_id,
                "name": actor_class.split(":")[0],
                "language": "python"
            })

        # Add methods
        for method_info in self.methods.get(job_id, {}).values():
            graph_data["methods"].append({
                "id": method_info["id"],
                "actorId": method_info["actorId"],
                "name": method_info["name"],
                "language": "python"
            })

        # Add functions
        for func_name, function_id in self.function_id_map.get(job_id, {}).items():
            if "." not in func_name:  # Ensure it's not a method
                graph_data["functions"].append({
                    "id": function_id,
                    "name": func_name,
                    "language": "python"
                })

        # Add call flows
        for call_edge, count in self.call_graph.get(job_id, {}).items():
            caller, callee = call_edge.split("->")

            # Get source ID
            source_id = None
            if caller in self.methods.get(job_id, {}):
                source_id = self.methods[job_id][caller]["id"]
            elif caller in self.function_id_map.get(job_id, {}):
                source_id = self.function_id_map[job_id][caller]

            # Get target ID
            target_id = None
            if callee in self.methods.get(job_id, {}):
                target_id = self.methods[job_id][callee]["id"]
            elif callee in self.function_id_map.get(job_id, {}):
                target_id = self.function_id_map[job_id][callee]

            if source_id and target_id:
                graph_data["callFlows"].append({
                    "source": source_id,
                    "target": target_id,
                    "count": count
                })

        return graph_data

def record_call(callee_class, callee_func):
    caller_class = None
    try:
        caller_actor = ray.get_runtime_context().current_actor
        if caller_actor is not None and hasattr(caller_actor, "_ray_actor_creation_function_descriptor"):
            caller_class = caller_actor._ray_actor_creation_function_descriptor.class_name +":"+ caller_actor._ray_actor_id.hex()
    except Exception:
        pass

    current_task_name = ray.get_runtime_context().get_task_name()
    if current_task_name is not None:
        caller_func = current_task_name.split(".")[-1]
    else:
        caller_func = "main"
    # Create a record for this call
    call_record = {
        "caller_class": caller_class,
        "caller_func": caller_func,
        "callee_class": callee_class,
        "callee_func": callee_func,
        "call_times": 1,
        "job_id": ray.get_runtime_context().get_job_id()
    }

    monitor = None
    try:
        monitor = ray.get_actor(name="insight_monitor", namespace="flowinsight")
    except ValueError:
        monitor = InsightMonitor.options(name="insight_monitor", namespace="flowinsight", lifetime="detached").remote()
    
    monitor.emit_call_record.remote(call_record)
 