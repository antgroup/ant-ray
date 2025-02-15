import time
from typing import Dict, List, Any
from collections import defaultdict

def calculate_resource_utilization(nodes: List[Dict]) -> Dict[str, float]:
    """Calculate overall resource utilization from node data."""
    total_cpu = 0
    used_cpu = 0
    total_memory = 0
    used_memory = 0
    total_gpu = 0
    used_gpu = 0
    has_gpu = False

    for node in nodes:
        if not node.get("alive", False):
            continue

        resources = node.get("resources", {})
        
        # CPU
        total_cpu += resources.get("CPU", 0)
        used_cpu += resources.get("CPU_used", 0)
        
        # Memory
        total_memory += resources.get("memory", 0)
        used_memory += resources.get("memory_used", 0)
        
        # GPU if available
        if "GPU" in resources:
            has_gpu = True
            total_gpu += resources.get("GPU", 0)
            used_gpu += resources.get("GPU_used", 0)

    return {
        "cpu": used_cpu / total_cpu if total_cpu > 0 else 0,
        "memory": used_memory / total_memory if total_memory > 0 else 0,
        "gpu": used_gpu / total_gpu if has_gpu and total_gpu > 0 else None,
    }

def calculate_node_metrics(nodes: List[Dict]) -> Dict[str, Any]:
    """Calculate detailed node metrics."""
    now = time.time()
    window = 300  # 5 minutes
    start_time = now - window
    
    # Initialize time series data
    timestamps = []
    cpu_usage = []
    memory_usage = []
    network_received = []
    network_sent = []
    
    # Calculate metrics for each time point
    for t in range(int(start_time), int(now), 10):  # 10-second intervals
        cpu_total = 0
        cpu_used = 0
        memory_total = 0
        memory_used = 0
        net_rx = 0
        net_tx = 0
        
        for node in nodes:
            if not node.get("alive", False):
                continue
                
            metrics = node.get("metrics", {})
            timestamp_metrics = metrics.get(str(t), {})
            
            cpu_total += timestamp_metrics.get("cpu_total", 0)
            cpu_used += timestamp_metrics.get("cpu_used", 0)
            memory_total += timestamp_metrics.get("memory_total", 0)
            memory_used += timestamp_metrics.get("memory_used", 0)
            net_rx += timestamp_metrics.get("network_received", 0)
            net_tx += timestamp_metrics.get("network_sent", 0)
        
        timestamps.append(t * 1000)  # Convert to milliseconds for frontend
        cpu_usage.append(cpu_used / cpu_total if cpu_total > 0 else 0)
        memory_usage.append(memory_used / memory_total if memory_total > 0 else 0)
        network_received.append(net_rx)
        network_sent.append(net_tx)
    
    return {
        "timestamps": timestamps,
        "cpuUsage": cpu_usage,
        "memoryUsage": memory_usage,
        "networkReceived": network_received,
        "networkSent": network_sent,
        "totalNodes": len(nodes),
        "activeNodes": sum(1 for node in nodes if node.get("alive", False)),
        "totalCpuCores": sum(node.get("resources", {}).get("CPU", 0) for node in nodes),
        "totalMemoryGB": sum(node.get("resources", {}).get("memory", 0) / (1024 * 1024 * 1024) for node in nodes),
    }

def calculate_actor_stats(actors: List[Dict]) -> Dict[str, Any]:
    """Calculate actor statistics grouped by class."""
    summary = {
        "alive": 0,
        "dead": 0,
        "restarting": 0,
    }
    
    groups = defaultdict(lambda: {
        "name": "",
        "cpuUsage": 0,
        "memoryUsage": 0,
        "numActors": 0,
    })
    
    for actor in actors:
        # Update summary
        state = actor.get("state", "DEAD").upper()
        if state == "ALIVE":
            summary["alive"] += 1
        elif state == "RESTARTING":
            summary["restarting"] += 1
        else:
            summary["dead"] += 1
        
        # Update group stats
        class_name = actor.get("class_name", "Unknown")
        group = groups[class_name]
        group["name"] = class_name
        group["cpuUsage"] += actor.get("cpu_usage", 0)
        group["memoryUsage"] += actor.get("memory_usage", 0)
        group["numActors"] += 1
    
    return {
        "summary": summary,
        "groups": list(groups.values()),
    }

def calculate_job_stats(jobs: List[Dict]) -> Dict[str, Any]:
    """Calculate job statistics and metrics."""
    summary = {
        "total": len(jobs),
        "pending": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
    }
    
    durations = []
    
    for job in jobs:
        status = job.get("status", "PENDING").upper()
        summary[status.lower()] += 1
        
        if status in ["SUCCEEDED", "FAILED"]:
            start_time = job.get("start_time", 0)
            end_time = job.get("end_time", 0)
            if start_time and end_time:
                durations.append(end_time - start_time)
    
    # Calculate duration histogram
    if durations:
        min_duration = min(durations)
        max_duration = max(durations)
        bin_count = 10
        bin_size = (max_duration - min_duration) / bin_count if max_duration > min_duration else 1
        
        histogram = []
        for i in range(bin_count):
            start = min_duration + (i * bin_size)
            end = start + bin_size
            count = sum(1 for d in durations if start <= d < end)
            histogram.append({"start": start, "end": end, "count": count})
    else:
        histogram = []
    
    return {
        **summary,
        "averageDuration": sum(durations) / len(durations) if durations else 0,
        "durationHistogram": histogram,
    }

def calculate_advanced_metrics(nodes: List[Dict], actors: List[Dict], jobs: List[Dict]) -> Dict[str, Any]:
    """Calculate advanced analytics and correlations."""
    # Collect samples for correlation analysis
    samples = []
    now = time.time()
    window = 300  # 5 minutes
    start_time = now - window
    
    for t in range(int(start_time), int(now), 10):
        cpu_usage = 0
        cpu_total = 0
        task_count = 0
        
        for node in nodes:
            if not node.get("alive", False):
                continue
                
            metrics = node.get("metrics", {})
            timestamp_metrics = metrics.get(str(t), {})
            
            cpu_total += timestamp_metrics.get("cpu_total", 0)
            cpu_usage += timestamp_metrics.get("cpu_used", 0)
            task_count += timestamp_metrics.get("task_count", 0)
        
        if cpu_total > 0:
            samples.append({
                "timestamp": t * 1000,
                "cpuUsage": cpu_usage / cpu_total,
                "taskThroughput": task_count / 10,  # Tasks per second
            })
    
    # Calculate task success rate
    total_tasks = 0
    successful_tasks = 0
    for job in jobs:
        total_tasks += job.get("task_count", 0)
        successful_tasks += job.get("successful_tasks", 0)
    
    task_success_rate = successful_tasks / total_tasks if total_tasks > 0 else 1
    
    # Calculate resource efficiency
    resource_usage = calculate_resource_utilization(nodes)
    resource_efficiency = (resource_usage["cpu"] + resource_usage["memory"]) / 2
    
    # Generate insights
    insights = []
    if resource_efficiency < 0.3:
        insights.append("Low resource utilization detected. Consider reducing cluster size.")
    elif resource_efficiency > 0.8:
        insights.append("High resource utilization. Consider scaling up the cluster.")
    
    if task_success_rate < 0.95:
        insights.append("Task failure rate is above normal. Check logs for errors.")
    
    return {
        "samples": samples,
        "averageTaskLatency": sum(job.get("average_task_latency", 0) for job in jobs) / len(jobs) if jobs else 0,
        "taskSuccessRate": task_success_rate,
        "resourceEfficiency": resource_efficiency,
        "insights": insights,
    } 