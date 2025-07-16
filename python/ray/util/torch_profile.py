import functools
import os
import threading
import queue

from ray.util.insight import is_flow_insight_enabled
from ray.util.torch_profile_base import (
    check_torch_availability,
    write_profile_result_to_file,
    profile_function_sync,
    profile_function_async,
)


def _get_global_worker():
    """Get the current Ray global worker."""
    try:
        import ray._private.worker
        return ray._private.worker.global_worker
    except (ImportError, AttributeError):
        return None


def _get_ray_profile_log_dir():
    """Get the Ray-specific profile log directory path."""
    try:
        import ray._private.worker
        import ray._private.utils
        
        session_id = ray._private.worker._global_node.session_name
        log_dir = os.path.join(
            ray._private.utils.get_ray_temp_dir(), 
            session_id, 
            "flowinsight"
        )
        os.makedirs(log_dir, exist_ok=True)
        return log_dir
    except Exception:
        # Fallback to temp directory if Ray not available
        import tempfile
        log_dir = os.path.join(tempfile.gettempdir(), "torch_profile")
        os.makedirs(log_dir, exist_ok=True)
        return log_dir


def _profile_processor_worker(profile_queue, shutdown_event):
    """Background worker thread that processes the profile queue."""
    log_dir = _get_ray_profile_log_dir()
    
    while not shutdown_event.is_set():
        try:
            # Wait for profile data with timeout
            profile_data = profile_queue.get(timeout=1.0)
            if profile_data is None:  # Sentinel value for shutdown
                break
                
            write_profile_result_to_file(profile_data, log_dir)
            profile_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Warning: Profile processor worker error: {e}")


def _ensure_background_thread():
    """Ensure the background processing thread is running for the current worker."""
    global_worker = _get_global_worker()
    if global_worker is None:
        # Fallback to basic processing if no global worker available
        return None
    
    # Initialize worker-specific profiling attributes if not present
    if not hasattr(global_worker, '_torch_profile_queue'):
        global_worker._torch_profile_queue = queue.Queue()
        global_worker._torch_profile_shutdown_event = threading.Event()
        global_worker._torch_profile_thread = None
        global_worker._torch_profile_thread_lock = threading.Lock()
    
    with global_worker._torch_profile_thread_lock:
        if (global_worker._torch_profile_thread is None or 
            not global_worker._torch_profile_thread.is_alive()):
            
            global_worker._torch_profile_shutdown_event.clear()
            global_worker._torch_profile_thread = threading.Thread(
                target=_profile_processor_worker,
                args=(global_worker._torch_profile_queue, global_worker._torch_profile_shutdown_event),
                daemon=True,
                name=f"torch_profile_processor_{os.getpid()}"
            )
            global_worker._torch_profile_thread.start()
    
    return global_worker



def _enqueue_profile_result(profile_data: dict):
    """Enqueue profile result for async processing."""
    try:
        global_worker = _ensure_background_thread()
        if global_worker is None:
            print("Warning: No global worker available for profiling")
            return
        
        # Enqueue the profile data
        global_worker._torch_profile_queue.put(profile_data, block=False)
        
    except queue.Full:
        print("Warning: Profile queue is full, dropping profile result")
    except Exception as e:
        print(f"Warning: Failed to enqueue profile result: {e}")


def torch_profile(func):
    """
    Ray-integrated decorator to profile PyTorch functions with comprehensive detailed profiling.
    Results are processed asynchronously in the background to minimize overhead.
    Only active when Ray flow insight is enabled.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        if not check_torch_availability() or not is_flow_insight_enabled():
            return func(*args, **kwargs)
        
        return profile_function_sync(
            func, 
            log_dir=None, 
            enable_async_logging=True,
            enqueue_callback=_enqueue_profile_result
        )(*args, **kwargs)
    
    return wrapper


def async_torch_profile(func):
    """
    Ray-integrated async decorator to profile PyTorch functions with comprehensive detailed profiling.
    Results are processed asynchronously in the background to minimize overhead.
    Only active when Ray flow insight is enabled.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        if not check_torch_availability() or not is_flow_insight_enabled():
            return await func(*args, **kwargs)
        
        return await profile_function_async(
            func,
            log_dir=None,
            enable_async_logging=True,
            enqueue_callback=_enqueue_profile_result
        )(*args, **kwargs)
    
    return wrapper

