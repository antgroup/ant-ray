# flake8: noqa
# Ray should be imported before streaming
import ray


def _update_modules():
    try:
        import raystreaming
        import raystreaming.context
        import sys
        ray_streaming_module_name = raystreaming.__name__
        ray_streaming_modules = {}
        for mod_name, module in sys.modules.items():
            if mod_name.startswith(ray_streaming_module_name):
                ray_streaming_modules[mod_name.replace(
                    "raystreaming", "ray.streaming")] = module
        sys.modules.update(ray_streaming_modules)
    except Exception as e:
        print("import raystreaming error: ", e)


_update_modules()

try:
    from raystreaming._streaming import set_log_config

except Exception as e:
    print("import error: ", e)

__all__ = ["set_log_config", "StreamingContext"]
