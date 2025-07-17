import functools
import os

from ray.util.insight import is_flow_insight_enabled
from ray.util.torch_profile_base import (
    check_torch_availability,
    profile_function_sync,
    profile_function_async,
)


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


def torch_profile(func):
    """
    Ray-integrated decorator to profile PyTorch functions.
    Results are written directly to files in the Ray log directory.
    Only active when Ray flow insight is enabled.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        if not check_torch_availability() or not is_flow_insight_enabled():
            return func(*args, **kwargs)
        
        # Get Ray log directory
        log_dir = _get_ray_profile_log_dir()
        
        # Use the simplified profiling function
        return profile_function_sync(func, log_dir=log_dir)(*args, **kwargs)
    
    return wrapper


def async_torch_profile(func):
    """
    Ray-integrated async decorator to profile PyTorch functions.
    Results are written directly to files in the Ray log directory.
    Only active when Ray flow insight is enabled.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Skip profiling if torch is not available or flow insight is not enabled
        if not check_torch_availability() or not is_flow_insight_enabled():
            return await func(*args, **kwargs)
        
        # Get Ray log directory
        log_dir = _get_ray_profile_log_dir()
        
        # Use the simplified async profiling function
        return await profile_function_async(func, log_dir=log_dir)(*args, **kwargs)
    
    return wrapper

