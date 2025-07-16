import functools
import time
from dataclasses import dataclass
from typing import List, Optional, Any

try:
    import torch
    import torch.profiler
    _TORCH_AVAILABLE = True
except ImportError:
    _TORCH_AVAILABLE = False

from ray.util.insight import is_flow_insight_enabled


@dataclass
class ProfileResult:
    """Results from torch profiling."""
    
    function_name: str
    execution_time: float
    summary_table: str
    memory_summary: Optional[str] = None
    
    def print_results(self) -> None:
        """Print profiling results to stdout."""
        print("=" * 80)
        print(f"🔥 TORCH PROFILER RESULTS: {self.function_name}")
        print("=" * 80)
        print(f"⏱️  Total Execution Time: {self.execution_time:.4f}s")
        
        print("\n📊 PERFORMANCE SUMMARY:")
        print("-" * 60)
        print(self.summary_table)
        
        if self.memory_summary:
            print("\n💾 MEMORY SUMMARY:")
            print("-" * 60)
            print(self.memory_summary)
        
        print("=" * 80)


def torch_profile(func):
    """
    Decorator to profile PyTorch functions with detailed profiling.
    Skips profiling if torch is not installed or RAY_FLOW_INSIGHT is not enabled.
    
    Usage:
        @torch_profile
        def my_function(x):
            return model(x)
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        if not _TORCH_AVAILABLE or not is_flow_insight_enabled():
            return func(*args, **kwargs)
        
        function_name = func.__name__
        
        # Determine available activities
        activities = [torch.profiler.ProfilerActivity.CPU]
        if torch.cuda.is_available():
            activities.append(torch.profiler.ProfilerActivity.CUDA)
        
        start_time = time.time()
        
        with torch.profiler.profile(
            activities=activities,
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
        ) as prof:
            with torch.profiler.record_function(function_name):
                result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Generate summary table
        summary_table = prof.key_averages(group_by_input_shape=True).table(
            sort_by="cpu_time_total",
            row_limit=30
        )
        
        # Generate memory summary
        memory_summary = None
        try:
            memory_summary = prof.key_averages().table(
                sort_by="cpu_memory_usage",
                row_limit=10
            )
        except Exception:
            memory_summary = "Memory profiling data not available"
        
        # Create and display results
        profile_result = ProfileResult(
            function_name=function_name,
            execution_time=execution_time,
            summary_table=summary_table,
            memory_summary=memory_summary
        )
        
        profile_result.print_results()
        
        return result
    
    return wrapper
