import functools
import time
import os
import json
import threading
import fcntl
import uuid
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any, Callable
import tempfile

# Cache CUDA availability to avoid repeated checks
_cuda_available = None


def _check_cuda_availability():
    """Check CUDA availability once and cache the result."""
    global _cuda_available
    if _cuda_available is None:
        try:
            import torch.cuda
            _cuda_available = torch.cuda.is_available()
        except Exception:
            _cuda_available = False
    return _cuda_available


def _get_profile_metadata():
    """Get detailed metadata for the current execution context."""
    import inspect
    import threading
    
    # Get caller frame information
    frame = inspect.currentframe()
    caller_frame = None
    try:
        # Go up the call stack to find the actual caller
        for _ in range(5):  # Look up to 5 frames up
            frame = frame.f_back
            if frame and frame.f_code.co_filename not in [__file__, __file__.replace('_base', '')]:
                caller_frame = frame
                break
    except:
        pass
    
    metadata = {
        'pid': os.getpid(),
        'tid': threading.get_ident(),
        'timestamp': time.time(),
        'hostname': os.uname().nodename if hasattr(os, 'uname') else 'unknown',
    }
    
    if caller_frame:
        metadata.update({
            'caller_file': caller_frame.f_code.co_filename,
            'caller_function': caller_frame.f_code.co_name,
            'caller_line': caller_frame.f_lineno,
        })
    
    return metadata


def write_profile_result_to_file(profile_data: dict, log_dir: str):
    """Write profile result to JSONL file with file locking."""
    try:
        log_file = os.path.join(log_dir, "torch_profiles.jsonl")
        
        # Add execution metadata
        profile_data['metadata'] = _get_profile_metadata()
        profile_data['log_uuid'] = str(uuid.uuid4())
        
        # Write to file with lock
        with open(log_file, 'a') as f:
            # Lock the file for exclusive access
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                json.dump(profile_data, f, separators=(',', ':'))
                f.write('\n')
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                
    except Exception as e:
        print(f"Warning: Failed to write profile result to file: {e}")


@dataclass
class StackFrame:
    """Represents a single stack frame."""
    filename: str
    line_number: int
    function_name: str
    module_name: Optional[str] = None
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class OperationProfile:
    """Enhanced profile data for a single operation with complete details."""
    
    name: str
    cpu_time_self: float  # microseconds
    cpu_time_total: float  # microseconds 
    cuda_time_self: Optional[float] = None  # microseconds
    cuda_time_total: Optional[float] = None  # microseconds
    count: int = 0
    cpu_memory_usage: int = 0  # bytes
    cuda_memory_usage: int = 0  # bytes
    flops: Optional[int] = None
    input_shapes: Optional[List[List[int]]] = None
    
    # Enhanced details
    stack_trace: List[StackFrame] = field(default_factory=list)
    node_id: Optional[int] = None
    thread_id: Optional[int] = None
    sequence_number: Optional[int] = None
    device_type: Optional[str] = None
    device_index: Optional[int] = None
    is_async: bool = False
    kernel_name: Optional[str] = None
    module_hierarchy: Optional[str] = None
    
    # Timing details
    start_time_relative: Optional[float] = None  # microseconds from profiling start
    end_time_relative: Optional[float] = None    # microseconds from profiling start
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert stack_trace to dictionaries
        if self.stack_trace:
            data['stack_trace'] = [frame.to_dict() for frame in self.stack_trace]
        return data


@dataclass 
class ProfileResult:
    """Enhanced results from torch profiling with complete details."""
    
    function_name: str
    execution_time: float  # seconds
    total_cpu_time: float  # microseconds
    total_cuda_time: Optional[float] = None  # microseconds
    peak_memory_usage: int = 0  # bytes
    total_flops: Optional[int] = None
    operation_profiles: List[OperationProfile] = field(default_factory=list)
    top_cpu_operations: List[OperationProfile] = field(default_factory=list)
    top_memory_operations: List[OperationProfile] = field(default_factory=list)
    
    # Enhanced profiling metadata
    profiling_start_time: Optional[float] = None  # absolute timestamp
    profiling_end_time: Optional[float] = None    # absolute timestamp
    cuda_enabled: bool = False
    total_operations: int = 0
    unique_operations: int = 0
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert operation profiles to dictionaries
        data['operation_profiles'] = [op.to_dict() for op in self.operation_profiles]
        data['top_cpu_operations'] = [op.to_dict() for op in self.top_cpu_operations]
        data['top_memory_operations'] = [op.to_dict() for op in self.top_memory_operations]
        return data
    
    def save_to_file(self, log_dir: str):
        """Save this profile result to file."""
        try:
            profile_data = self.to_dict()
            write_profile_result_to_file(profile_data, log_dir)
        except Exception as e:
            print(f"Warning: Failed to save profile result: {e}")
    
    def print_results(self, show_stack_traces: bool = False, max_stack_depth: int = 5) -> None:
        """Print comprehensive profiling results to stdout."""
        print(f"\n=== Enhanced PyTorch Profiling Results for {self.function_name} ===")
        print(f"Total execution time: {self.execution_time:.4f}s")
        print(f"Total CPU time: {self.total_cpu_time/1000:.2f}ms")
        if self.total_cuda_time:
            print(f"Total CUDA time: {self.total_cuda_time/1000:.2f}ms")
        print(f"Peak memory usage: {self.peak_memory_usage/1024/1024:.2f}MB")
        if self.total_flops:
            print(f"Total FLOPs: {self.total_flops:,}")
        print(f"Total operations: {self.total_operations}")
        print(f"Unique operations: {self.unique_operations}")
        
        if self.profiling_start_time and self.profiling_end_time:
            print(f"Profiling window: {self.profiling_start_time:.6f}s to {self.profiling_end_time:.6f}s")
        
        print(f"\nTop CPU operations ({len(self.top_cpu_operations)}):")
        for i, op in enumerate(self.top_cpu_operations[:10], 1):
            extra_info = []
            if op.thread_id is not None:
                extra_info.append(f"thread:{op.thread_id}")
            if op.device_type:
                extra_info.append(f"device:{op.device_type}")
            if op.kernel_name:
                extra_info.append(f"kernel:{op.kernel_name}")
            
            extra_str = f" [{', '.join(extra_info)}]" if extra_info else ""
            print(f"  {i}. {op.name}: {op.cpu_time_total/1000:.2f}ms (calls: {op.count}){extra_str}")
            
            if show_stack_traces and op.stack_trace:
                print(f"     Stack trace (top {min(max_stack_depth, len(op.stack_trace))}):")
                for j, frame in enumerate(op.stack_trace[:max_stack_depth]):
                    print(f"       {j+1}. {frame.filename}:{frame.line_number} in {frame.function_name}")
        
        if any(op.cuda_time_total for op in self.operation_profiles):
            print(f"\nTop CUDA operations:")
            cuda_ops = [op for op in self.operation_profiles if op.cuda_time_total and op.cuda_time_total > 0]
            cuda_ops.sort(key=lambda x: x.cuda_time_total, reverse=True)
            for i, op in enumerate(cuda_ops[:10], 1):
                extra_info = []
                if op.device_index is not None:
                    extra_info.append(f"gpu:{op.device_index}")
                if op.kernel_name:
                    extra_info.append(f"kernel:{op.kernel_name}")
                if op.is_async:
                    extra_info.append("async")
                
                extra_str = f" [{', '.join(extra_info)}]" if extra_info else ""
                print(f"  {i}. {op.name}: {op.cuda_time_total/1000:.2f}ms (calls: {op.count}){extra_str}")
        
        if self.top_memory_operations:
            print(f"\nTop memory operations:")
            for i, op in enumerate(self.top_memory_operations[:10], 1):
                memory_mb = op.cpu_memory_usage / 1024 / 1024
                cuda_memory_mb = op.cuda_memory_usage / 1024 / 1024 if op.cuda_memory_usage else 0
                memory_str = f"{memory_mb:.2f}MB CPU"
                if cuda_memory_mb > 0:
                    memory_str += f", {cuda_memory_mb:.2f}MB CUDA"
                print(f"  {i}. {op.name}: {memory_str} (calls: {op.count})")


def check_torch_availability():
    """Check torch availability for profiling."""
    try:
        import torch
        import torch.profiler
        return True
    except ImportError:
        return False


def _safe_getattr(obj, attr, default=None):
    """Safely get attribute with fallback for different PyTorch versions."""
    try:
        return getattr(obj, attr, default)
    except (AttributeError, RuntimeError):
        return default


def _extract_stack_trace(event) -> List[StackFrame]:
    """Extract stack trace from profiler event with robust error handling."""
    stack_trace = []
    try:
        if not hasattr(event, 'stack') or not event.stack:
            return stack_trace
            
        for frame in event.stack:
            try:
                # Different PyTorch versions may have different attribute names
                filename = _safe_getattr(frame, 'filename') or _safe_getattr(frame, 'file_name', 'unknown')
                line_number = _safe_getattr(frame, 'line') or _safe_getattr(frame, 'line_number', 0)
                function_name = _safe_getattr(frame, 'name') or _safe_getattr(frame, 'function_name', 'unknown')
                module_name = _safe_getattr(frame, 'module') or _safe_getattr(frame, 'module_name')
                
                if filename != 'unknown' and line_number > 0:
                    stack_frame = StackFrame(
                        filename=str(filename),
                        line_number=int(line_number),
                        function_name=str(function_name),
                        module_name=str(module_name) if module_name else None
                    )
                    stack_trace.append(stack_frame)
            except (AttributeError, ValueError, TypeError):
                continue
                
    except Exception:
        # If stack extraction fails completely, return empty list
        pass
        
    return stack_trace


def _extract_operation_profiles(prof) -> List[OperationProfile]:
    """Extract comprehensive operation profiles from torch profiler with robust error handling."""
    operations = []
    
    try:
        key_averages = prof.key_averages(group_by_input_shape=True)
        
        for event in key_averages:
            try:
                # Extract stack trace with robust error handling
                stack_trace = _extract_stack_trace(event)
                
                # Extract input shapes if available
                input_shapes = None
                try:
                    if hasattr(event, 'input_shapes') and event.input_shapes:
                        input_shapes = [[int(dim) for dim in shape] for shape in event.input_shapes]
                except (ValueError, TypeError):
                    input_shapes = None
                
                # Extract device information with fallbacks
                device_type = _safe_getattr(event, 'device_type')
                if device_type is not None:
                    device_type = str(device_type)
                device_index = _safe_getattr(event, 'device_index')
                
                # Extract timing details with fallbacks
                start_time_rel = None
                end_time_rel = None
                try:
                    time_range = _safe_getattr(event, 'time_range', {})
                    if isinstance(time_range, dict):
                        start_time_rel = time_range.get('start')
                        end_time_rel = time_range.get('end')
                except (AttributeError, TypeError):
                    pass
                
                # Extract all attributes safely
                operation = OperationProfile(
                    name=str(event.key),
                    cpu_time_self=float(_safe_getattr(event, 'cpu_time', 0)),
                    cpu_time_total=float(_safe_getattr(event, 'cpu_time_total', 0)),
                    cuda_time_self=_safe_getattr(event, 'device_time') if hasattr(event, 'device_time') else _safe_getattr(event, 'cuda_time'),
                    cuda_time_total=_safe_getattr(event, 'device_time_total') if hasattr(event, 'device_time_total') else _safe_getattr(event, 'cuda_time_total'),
                    count=int(_safe_getattr(event, 'count', 0)),
                    cpu_memory_usage=int(_safe_getattr(event, 'cpu_memory_usage', 0)),
                    cuda_memory_usage=int(_safe_getattr(event, 'cuda_memory_usage', 0)),
                    flops=_safe_getattr(event, 'flops'),
                    input_shapes=input_shapes,
                    
                    # Enhanced details with safe extraction
                    stack_trace=stack_trace,
                    node_id=_safe_getattr(event, 'node_id'),
                    thread_id=_safe_getattr(event, 'thread_id'),
                    sequence_number=_safe_getattr(event, 'sequence_nr'),
                    device_type=device_type,
                    device_index=device_index,
                    is_async=bool(_safe_getattr(event, 'is_async', False)),
                    kernel_name=_safe_getattr(event, 'kernel'),
                    module_hierarchy=_safe_getattr(event, 'module_hierarchy'),
                    start_time_relative=start_time_rel,
                    end_time_relative=end_time_rel
                )
                operations.append(operation)
                
            except Exception as e:
                # Skip individual events that fail to parse
                print(f"Warning: Skipped profiling event due to extraction error: {e}")
                continue
                
    except Exception as e:
        print(f"Warning: Could not extract detailed profiling data: {e}")
    
    return operations


def create_profile_result(prof, function_name: str, execution_time: float, 
                         start_timestamp: float, end_timestamp: float) -> ProfileResult:
    """Create a comprehensive ProfileResult from torch profiler data."""
    operations = _extract_operation_profiles(prof)
    
    # Calculate totals with safe arithmetic
    total_cpu_time = sum(op.cpu_time_total for op in operations if op.cpu_time_total)
    total_cuda_time = None
    cuda_times = [op.cuda_time_total for op in operations if op.cuda_time_total and op.cuda_time_total > 0]
    if cuda_times:
        total_cuda_time = sum(cuda_times)
    
    # Calculate peak memory usage
    cpu_memories = [op.cpu_memory_usage for op in operations if op.cpu_memory_usage > 0]
    cuda_memories = [op.cuda_memory_usage for op in operations if op.cuda_memory_usage > 0]
    peak_memory = max(cpu_memories + cuda_memories) if (cpu_memories or cuda_memories) else 0
    
    # Calculate total FLOPs
    flops_list = [op.flops for op in operations if op.flops and op.flops > 0]
    total_flops = sum(flops_list) if flops_list else None
    
    # Sort for top operations
    cpu_ops = sorted(
        [op for op in operations if op.cpu_time_total > 0], 
        key=lambda x: x.cpu_time_total, 
        reverse=True
    )
    
    memory_ops = sorted(
        [op for op in operations if (op.cpu_memory_usage + op.cuda_memory_usage) > 0], 
        key=lambda x: (x.cpu_memory_usage + x.cuda_memory_usage), 
        reverse=True
    )
    
    # Count unique operations
    unique_ops = len(set(op.name for op in operations))
    
    return ProfileResult(
        function_name=function_name,
        execution_time=execution_time,
        total_cpu_time=total_cpu_time,
        total_cuda_time=total_cuda_time,
        peak_memory_usage=peak_memory,
        total_flops=total_flops,
        operation_profiles=operations,
        top_cpu_operations=cpu_ops,
        top_memory_operations=memory_ops,
        
        # Enhanced metadata
        profiling_start_time=start_timestamp,
        profiling_end_time=end_timestamp,
        cuda_enabled=_check_cuda_availability(),
        total_operations=len(operations),
        unique_operations=unique_ops
    )


def profile_function_sync(func: Callable, 
                         enqueue_callback: Optional[Callable[[dict], None]] = None) -> Callable:
    """
    Synchronous torch profiling decorator with flexible logging options.
    
    Args:
        func: Function to profile
        enqueue_callback: Optional callback for custom async processing
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available
        if not check_torch_availability():
            return func(*args, **kwargs)
        
        import torch
        import torch.profiler
        
        function_name = func.__name__
        
        # Determine available activities
        activities = [torch.profiler.ProfilerActivity.CPU]
        if _check_cuda_availability():
            activities.append(torch.profiler.ProfilerActivity.CUDA)
        
        start_time = time.time()
        
        try:
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
            
            # Create profile result
            profile_result = create_profile_result(prof, function_name, execution_time, start_time, end_time)
            
            # Handle logging based on configuration
            if enqueue_callback:
                # Use custom async callback
                enqueue_callback(profile_result.to_dict())
            
        except Exception as e:
            print(f"Warning: PyTorch profiling failed for {function_name}: {e}")
            # Execute function normally if profiling fails
            result = func(*args, **kwargs)
        
        return result
    
    return wrapper


def profile_function_async(func: Callable,
                          enqueue_callback: Optional[Callable[[dict], None]] = None) -> Callable:
    """
    Asynchronous torch profiling decorator with flexible logging options.
    
    Args:
        func: Async function to profile
        enqueue_callback: Optional callback for custom async processing
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available
        if not check_torch_availability():
            return await func(*args, **kwargs)
        
        import torch
        import torch.profiler
        
        function_name = func.__name__
        
        # Determine available activities
        activities = [torch.profiler.ProfilerActivity.CPU]
        if _check_cuda_availability():
            activities.append(torch.profiler.ProfilerActivity.CUDA)
        
        start_time = time.time()
        
        try:
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
            
            # Create profile result
            profile_result = create_profile_result(prof, function_name, execution_time, start_time, end_time)
            
            # Handle logging based on configuration
            if enqueue_callback:
                # Use custom async callback
                enqueue_callback(profile_result.to_dict())
            
        except Exception as e:
            print(f"Warning: PyTorch profiling failed for {function_name}: {e}")
            # Execute function normally if profiling fails
            result = await func(*args, **kwargs)
        
        return result
    
    return wrapper 
