import functools
import time
from dataclasses import dataclass
from typing import List, Optional, Any, Set
import inspect
import os

from ray.util.insight import is_flow_insight_enabled

# Store original functions to avoid infinite recursion
_original_functions = {}
_patched_functions: Set[str] = set()
_cuda_available = None  # Cache CUDA availability to avoid recursion


def _get_function_from_path(func_path):
    """Get the actual function object from a string path like 'torch.matmul'."""
    try:
        import torch
        module_parts = func_path.split('.')
        current_obj = torch
        
        for part in module_parts[1:]:  # Skip 'torch'
            current_obj = getattr(current_obj, part)
        
        return current_obj
    except (AttributeError, ImportError):
        return None


def _check_cuda_availability():
    """Check CUDA availability once and cache the result."""
    global _cuda_available
    if _cuda_available is None:
        try:
            # Use the original function to avoid recursion
            if 'torch.cuda.is_available' in _original_functions:
                _cuda_available = _original_functions['torch.cuda.is_available']()
            else:
                import torch.cuda
                _cuda_available = torch.cuda.is_available()
        except Exception:
            _cuda_available = False
    return _cuda_available

def _create_profiled_wrapper(original_func, func_name):
    """Create a profiled wrapper for a torch function."""
    @functools.wraps(original_func)
    def wrapper(*args, **kwargs):
        # Use lightweight profiling
        start_time = time.time()
        result = original_func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        
        return result
    
    return wrapper


# Key torch operations to profile
_CRITICAL_OPS = {
    'torch.matmul', 'torch.mm', 'torch.bmm',
    'torch.nn.functional.linear',
    'torch.nn.functional.relu', 'torch.nn.functional.gelu',
    'torch.nn.functional.dropout',
    'torch.nn.functional.mse_loss', 'torch.nn.functional.cross_entropy',
    'torch.add', 'torch.mul', 'torch.sum', 'torch.mean',
}

# Key nn.Module classes to profile
_NN_MODULES_TO_PATCH = {
    'torch.nn.Linear', 'torch.nn.ReLU', 'torch.nn.Dropout', 'torch.nn.MSELoss',
    'torch.nn.Conv2d', 'torch.nn.BatchNorm2d', 'torch.nn.GELU',
}


def _patch_critical_ops():
    """Patch the critical operations for profiling."""
    try:
        import torch
        import torch.profiler
        _TORCH_AVAILABLE = True
    except ImportError:
        _TORCH_AVAILABLE = False


    if not _TORCH_AVAILABLE:
        return 0
    
    patched_count = 0
    for func_path in _CRITICAL_OPS:
        original_func = _get_function_from_path(func_path)
        if original_func is None:
            continue
            
        try:
            # Store original function
            _original_functions[func_path] = original_func
            
            # Create profiled wrapper
            profiled_func = _create_profiled_wrapper(original_func, func_path)
            
            # Get the module and function name to patch
            module_parts = func_path.split('.')
            
            # Navigate to the correct module
            current_module = torch
            for part in module_parts[1:-1]:  # Skip 'torch' and function name
                current_module = getattr(current_module, part)
            
            # Replace the function
            setattr(current_module, module_parts[-1], profiled_func)
            _patched_functions.add(func_path)
            patched_count += 1
            
        except (AttributeError, TypeError):
            continue
    
    return patched_count


def _patch_nn_modules():
    """Patch the forward methods of common nn.Module classes."""
    try:
        import torch
        import torch.profiler
        _TORCH_AVAILABLE = True
    except ImportError:
        _TORCH_AVAILABLE = False


    if not _TORCH_AVAILABLE:
        return 0
    
    patched_count = 0
    for module_path in _NN_MODULES_TO_PATCH:
        module_class = _get_function_from_path(module_path)
        if module_class is None or not inspect.isclass(module_class):
            continue
            
        try:
            # Get the original forward method
            original_forward = module_class.forward
            if original_forward is None:
                continue
            
            # Store original forward method
            forward_path = f"{module_path}.forward"
            _original_functions[forward_path] = original_forward
            
            # Create profiled wrapper for the forward method
            def create_forward_wrapper(orig_forward, mod_path):
                @functools.wraps(orig_forward)
                def profiled_forward(self, *args, **kwargs):
                    start_time = time.time()
                    result = orig_forward(self, *args, **kwargs)
                    end_time = time.time()
                    execution_time = end_time - start_time
                    
                    return result
                return profiled_forward
            
            # Replace the forward method
            profiled_forward = create_forward_wrapper(original_forward, module_path)
            setattr(module_class, 'forward', profiled_forward)
            _patched_functions.add(forward_path)
            patched_count += 1
            
        except (AttributeError, TypeError):
            continue
    
    return patched_count


def _install_torch_profiling():
    """Install torch operation profiling in the current Ray worker."""
    try:
        import torch
        import torch.profiler
        _TORCH_AVAILABLE = True
    except ImportError:
        _TORCH_AVAILABLE = False


    if not _TORCH_AVAILABLE:
        return
    
    # Patch functional operations
    func_patched = _patch_critical_ops()
    
    # Patch nn.Module forward methods
    module_patched = _patch_nn_modules()
    
    total_patched = func_patched + module_patched


@dataclass
class ProfileResult:
    """Results from torch profiling."""
    
    function_name: str
    execution_time: float
    summary_table: str
    memory_summary: Optional[str] = None
    
    def print_results(self) -> None:
        """Print profiling results to stdout."""
        pass


def torch_profile(func):
    """
    Decorator to profile PyTorch functions with detailed profiling.
    Automatically installs operation-level profiling in Ray workers.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        try:
            import torch
            import torch.profiler
            _TORCH_AVAILABLE = True
        except ImportError:
            _TORCH_AVAILABLE = False


        if not _TORCH_AVAILABLE or not is_flow_insight_enabled():
            return func(*args, **kwargs)
        
        _install_torch_profiling()
        
        function_name = func.__name__
        
        # Determine available activities
        activities = [torch.profiler.ProfilerActivity.CPU]
        if _check_cuda_availability():
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


def async_torch_profile(func):
    """
    Async decorator to profile PyTorch functions with detailed profiling.
    Automatically installs operation-level profiling in Ray workers.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        try:
            import torch
            import torch.profiler
            _TORCH_AVAILABLE = True
        except ImportError:
            _TORCH_AVAILABLE = False

        if not _TORCH_AVAILABLE or not is_flow_insight_enabled():
            return await func(*args, **kwargs)
        
        _install_torch_profiling()
        
        function_name = func.__name__
        
        # Determine available activities
        activities = [torch.profiler.ProfilerActivity.CPU]
        if _check_cuda_availability():
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
                result = await func(*args, **kwargs)
        
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
