import functools
import time
import os
import json
import tempfile
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass, asdict
from contextlib import contextmanager

# Handle PyTorch import gracefully
try:
    import torch
    import torch.profiler
    _TORCH_AVAILABLE = True
except ImportError:
    _TORCH_AVAILABLE = False
    torch = None


@dataclass
class ProfileResult:
    """Lightweight profile result data structure."""
    function_name: str
    execution_time_ms: float
    cpu_time_total_ms: float
    cuda_time_total_ms: float
    memory_usage_mb: float
    operation_count: int
    operations: List[Dict[str, Any]]
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


def check_torch_availability() -> bool:
    """Check if PyTorch is available for profiling."""
    return _TORCH_AVAILABLE


def _extract_profiling_data(prof) -> Dict[str, Any]:
    """Extract detailed profiling data from torch profiler."""
    if not _TORCH_AVAILABLE or prof is None:
        return {}
    
    try:
        # Get key averages - this contains the operation statistics
        events = prof.key_averages()
        
        if not events:
            return {}
        
        # Calculate totals
        total_cpu_time = sum(getattr(event, 'cpu_time_total', 0) for event in events)
        total_device_time = sum(getattr(event, 'device_time_total', 0) for event in events)
        total_cpu_memory = sum(getattr(event, 'cpu_memory_usage', 0) for event in events)
        total_device_memory = sum(getattr(event, 'device_memory_usage', 0) for event in events)
        total_self_cpu_time = sum(getattr(event, 'self_cpu_time_total', 0) for event in events)
        total_self_device_time = sum(getattr(event, 'self_device_time_total', 0) for event in events)
        total_self_cpu_memory = sum(getattr(event, 'self_cpu_memory_usage', 0) for event in events)
        total_self_device_memory = sum(getattr(event, 'self_device_memory_usage', 0) for event in events)
        total_flops = sum(getattr(event, 'flops', 0) for event in events)
        
        # Extract operations (sort by CPU time)
        operations = []
        sorted_events = sorted(events, key=lambda x: getattr(x, 'cpu_time_total', 0), reverse=True)
        
        for event in sorted_events:
            try:
                # Get input shapes if available
                input_shapes = getattr(event, 'input_shapes', [])
                if input_shapes:
                    input_shapes = [str(shape) for shape in input_shapes]
                
                op_data = {
                    'name': str(event.key),
                    'cpu_time_total_ms': getattr(event, 'cpu_time_total', 0) / 1000.0,
                    'device_time_total_ms': getattr(event, 'device_time_total', 0) / 1000.0,
                    'cpu_memory_usage_mb': getattr(event, 'cpu_memory_usage', 0) / (1024 * 1024),
                    'device_memory_usage_mb': getattr(event, 'device_memory_usage', 0) / (1024 * 1024),
                    'self_cpu_time_total_ms': getattr(event, 'self_cpu_time_total', 0) / 1000.0,
                    'self_device_time_total_ms': getattr(event, 'self_device_time_total', 0) / 1000.0,
                    'self_cpu_memory_usage_mb': getattr(event, 'self_cpu_memory_usage', 0) / (1024 * 1024),
                    'self_device_memory_usage_mb': getattr(event, 'self_device_memory_usage', 0) / (1024 * 1024),
                    'count': getattr(event, 'count', 0),
                    'flops': getattr(event, 'flops', 0),
                    'input_shapes': input_shapes,
                    'device_type': str(getattr(event, 'device_type', None)),
                    'is_async': getattr(event, 'is_async', False),
                    'is_user_annotation': getattr(event, 'is_user_annotation', False),
                    'node_id': getattr(event, 'node_id', None),
                    'scope': getattr(event, 'scope', None)
                }
                operations.append(op_data)
            except Exception:
                continue
        
        # Calculate efficiency metrics
        cpu_efficiency = (total_self_cpu_time / total_cpu_time * 100) if total_cpu_time > 0 else 0
        device_efficiency = (total_self_device_time / total_device_time * 100) if total_device_time > 0 else 0
        
        return {
            'cpu_time_total_ms': total_cpu_time / 1000.0,
            'cuda_time_total_ms': total_device_time / 1000.0,
            'self_cpu_time_total_ms': total_self_cpu_time / 1000.0,
            'self_device_time_total_ms': total_self_device_time / 1000.0,
            'memory_usage_mb': (total_cpu_memory + total_device_memory) / (1024 * 1024),
            'cpu_memory_usage_mb': total_cpu_memory / (1024 * 1024),
            'device_memory_usage_mb': total_device_memory / (1024 * 1024),
            'self_cpu_memory_usage_mb': total_self_cpu_memory / (1024 * 1024),
            'self_device_memory_usage_mb': total_self_device_memory / (1024 * 1024),
            'total_flops': total_flops,
            'gflops': total_flops / 1e9 if total_flops > 0 else 0,
            'cpu_efficiency_percent': cpu_efficiency,
            'device_efficiency_percent': device_efficiency,
            'operation_count': len(events),
            'operations': operations
        }
    
    except Exception as e:
        # Return error info for debugging
        return {'error': f'Failed to extract profiling data: {str(e)}'}


@contextmanager
def _torch_profiler_context():
    """Create a torch profiler context with optimal settings."""
    if not _TORCH_AVAILABLE:
        yield None
        return
    
    activities = [torch.profiler.ProfilerActivity.CPU]
    if torch.cuda.is_available():
        activities.append(torch.profiler.ProfilerActivity.CUDA)
    
    with torch.profiler.profile(
        activities=activities,
        record_shapes=False,  # Disable for performance
        profile_memory=True,
        with_stack=False,  # Disable for performance
        with_flops=False,  # Disable for performance
        with_modules=False  # Disable for performance
    ) as prof:
        yield prof


def _write_profile_data_to_file(profile_data: Dict[str, Any], log_dir: Optional[str] = None, function_name: Optional[str] = None) -> None:
    """Write profile data directly to file - simple and fast."""
    try:
        if log_dir is None:
            log_dir = tempfile.gettempdir()
            
        os.makedirs(log_dir, exist_ok=True)
        filename = f"{function_name}_{int(time.time())}.json"
        filepath = os.path.join(log_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(profile_data, f, indent=2)
    except Exception:
        pass  # Silently ignore write errors


def profile_function_sync(func: Callable, log_dir: Optional[str] = None) -> Callable:
    """
    Synchronous profiling decorator using torch.profiler.
    
    Args:
        func: Function to profile
        log_dir: Directory to write profile results. If None, uses temp directory.
        
    Returns:
        Wrapped function with profiling capability
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not _TORCH_AVAILABLE:
            return func(*args, **kwargs)
        
        start_time = time.time()
        
        with _torch_profiler_context() as prof:
            result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        
        # Extract profiling data and write to file
        profiling_data = _extract_profiling_data(prof)
        
        # Create the complete profile result
        profile_result = {
            'event_type': 'function_profile',
            'profile_data': {
                'function_name': func.__name__,
                'execution_time_ms': execution_time_ms,
                'timestamp': start_time,
                **profiling_data  # Include all extracted profiling data
            }
        }
        
        # Write directly to file
        _write_profile_data_to_file(profile_result, log_dir, func.__name__)
        
        return result
    
    return wrapper


def profile_function_async(func: Callable, log_dir: Optional[str] = None) -> Callable:
    """
    Asynchronous profiling decorator using torch.profiler.
    
    Args:
        func: Async function to profile
        log_dir: Directory to write profile results. If None, uses temp directory.
        
    Returns:
        Wrapped async function with profiling capability
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not _TORCH_AVAILABLE:
            return await func(*args, **kwargs)
        
        start_time = time.time()
        
        with _torch_profiler_context() as prof:
            result = await func(*args, **kwargs)
        
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        
        # Extract profiling data and write to file
        profiling_data = _extract_profiling_data(prof)
        
        # Create the complete profile result
        profile_result = {
            'event_type': 'function_profile',
            'profile_data': {
                'function_name': func.__name__,
                'execution_time_ms': execution_time_ms,
                'timestamp': start_time,
                **profiling_data  # Include all extracted profiling data
            }
        }
        
        # Write directly to file
        _write_profile_data_to_file(profile_result, log_dir, func.__name__)
        
        return result
    
    return wrapper

