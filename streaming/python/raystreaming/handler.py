import inspect
import logging
import textwrap
from abc import ABC, abstractmethod

__all__ = [
    "remote",
    "generator",
    "processor",
    "initializer",
    "TideHandler",
    "generate_template",
    "compatible",
]

logger = logging.getLogger(__name__)


def remote(*args, **kwargs):
    """Define a remote tide actor class"""
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        return make_decorator()(args[0])
    raise ValueError(f"args {args} and kwargs {kwargs} are not supported.")


# Keep this in sync with
# io.ray.tide.runtime.TideHandlers.generateTemplate
# generate_template = "__ray__tide__{}__"
generate_template = "{}"
# Return all annotated methods joined by `,`, so that driver/master can
# call this methods to know whether this actor has generator/processor
# or not.
get_annotated_name = generate_template.format("get_annotated")


def make_decorator():
    def decorator(cls):
        """Besides all methods in TideHandler, this decorator will inject a
        method named `__ray__tide__get_annotated__` which a list of all
        annotated methods tag separate by comma.
        """
        func_list = inspect.getmembers(
            TideHandler, predicate=inspect.isfunction)
        any_tide_func_name = func_list[0][0]
        # See `__ray_actor_class__` in python/ray/actor.py
        ray_modified = hasattr(cls, "__ray_actor_class__")
        ray_actor_class_instance = cls
        if ray_modified:
            cls = getattr(cls, "__ray_actor_class__")
        else:
            if not inspect.isclass(cls):
                raise TypeError(
                    f"{remote} can only be applied to a class instead "
                    f"of {cls}.")
        cls_functions = inspect.getmembers(cls, predicate=inspect.isfunction)
        for func_name, func in cls_functions:
            # If this class is already decorated, return directly
            if func_name == any_tide_func_name:
                return cls
        annotated_methods = {}
        cls_function_names = set()
        for func_name, func in cls_functions:
            cls_function_names.add(func_name)
            for decorator_name, tag in get_annotated_method_tags().items():
                if hasattr(func, tag):
                    if decorator_name in annotated_methods:
                        raise Exception(
                            f"Duplicate annotated method: {func} annotated by "
                            f"{decorator_name}. All annotated methods are "
                            f"{annotated_methods}")
                    annotated_methods[decorator_name] = func

        methods_code = []

        # get annotated methods code
        set_annotated_args = ", ".join(
            f"{decorator_name}={{self_name}}.{decorated_func.__name__}"
            for decorator_name, decorated_func in annotated_methods.items())
        get_annotated_code = textwrap.dedent(f"""
        def {get_annotated_name}({{self_name}}):
            return {repr(", ".join(annotated_methods.keys()))}
        """).format(self_name="self")
        methods_code.append((get_annotated_name, get_annotated_code))

        for func_name, func in func_list:
            sig = inspect.signature(func)
            self_name = get_call_args(sig)[0]
            call_args = get_call_args(sig)[1:]  # skip self
            args_code = ", ".join(call_args)
            generated_func_name = generate_template.format(func_name)
            assert generated_func_name not in cls_function_names, \
                f"class {cls} has a method named {generated_func_name} " \
                f"duplicated with ray tide handler generated method for " \
                f"`{func_name}`."
            method_code = textwrap.dedent(f"""
            def {generated_func_name}{str(sig)}:
                from raystreaming.runtime.worker import get_handler
                return get_handler({self_name}).{func_name}({args_code})
            """)
            if func_name == TideHandler.init.__name__:
                set_annotated_code = \
                    " " * 4 + \
                    f"get_handler({self_name})." \
                    f"{TideHandler.set_annotated_methods.__name__}" \
                    f"({set_annotated_args.format(self_name=self_name)})"
                set_annotated_code.format(self_name=self_name)
                lines = method_code.split("\n")
                line_index = next(i for i, line in enumerate(lines)
                                  if "import get_handler" in line)
                lines.insert(line_index + 1, set_annotated_code)
                method_code = "\n".join(lines)
            methods_code.append((generated_func_name, method_code))
        scope = {cls.__name__: cls}
        code = "\n".join([code for _, code in methods_code])
        logger.debug(f"Inject methods \n{code}\n to class {cls}")
        exec(code, scope)
        methods_to_inject = [scope[func_name] for func_name, _ in methods_code]
        for method in methods_to_inject:
            setattr(cls, method.__name__, method)
        return ray_actor_class_instance if ray_modified else cls

    return decorator


def get_call_args(sig):
    args = []
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            arg = f"*{param.name}"
        elif param.default != inspect.Parameter.empty:
            arg = f"{param.name}={param.name}"
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            arg = f"**{param.name}"
        else:
            arg = f"{param.name}"
        args.append(arg)
    return args


def get_annotated_method_tags():
    return {
        generator.__name__: "__ray__tide__is__generator__",
        processor.__name__: "__ray__tide__is__processor__",
        initializer.__name__: "__ray__tide__is__initializer__",
        finalizer.__name__: "__ray__tide__is__finalizer__",
        checkpoint.getter.__name__: "__ray__tide__is__checkpoint_getter__",
        checkpoint.setter.__name__: "__ray__tide__is__checkpoint_setter__"
    }


def generator(func):
    """Mark a tide actor method as a data generator method.
    A tide actor can only have one actor method annotated by this annotation.
    """
    setattr(func, get_annotated_method_tags()[generator.__name__], True)
    return func


def processor(func):
    """Mark a tide actor method as a data process method.
    A tide actor can only have one actor method annotated by this annotation.
    """
    setattr(func, get_annotated_method_tags()[processor.__name__], True)
    return func


def initializer(func):
    """Mark a tide actor method as a init method.
    A tide actor can only have one actor method annotated by this annotation.
    """
    setattr(func, get_annotated_method_tags()[initializer.__name__], True)
    return func


def finalizer(func):
    """Mark a tide actor method as a finish method.
    A tide actor can only have one actor method annotated by this annotation.
    """
    setattr(func, get_annotated_method_tags()[finalizer.__name__], True)
    return func


class checkpoint:
    @staticmethod
    def getter(func):
        setattr(func,
                get_annotated_method_tags()[checkpoint.getter.__name__], True)
        return func

    @staticmethod
    def setter(func):
        setattr(func,
                get_annotated_method_tags()[checkpoint.setter.__name__], True)
        return func


class TideHandler(ABC):
    @abstractmethod
    def set_annotated_methods(self, **methods):
        """
        This method run before :func:`TideHandler.init`
        all annotated methods: {"generator":xxx, "processor":xxx,
         "initializer":xxx, "finalizer":xxx, "getter":xxx, "setter": xxx}
        Args:
            **methods: all annotated methods
        """
        pass

    @abstractmethod
    def init(self, worker_config_str):
        pass

    @abstractmethod
    def register_context(self, worker_context_bytes):
        pass

    @abstractmethod
    def rollback(self, checkpoint_id_bytes, partial_checkpoint_id_bytes):
        pass

    @abstractmethod
    def on_reader_message(self, *buffers):
        """Called by upstream queue writer to send data message to downstream
        queue reader.
        """
        pass

    @abstractmethod
    def on_reader_message_sync(self, buffer: bytes):
        """Called by upstream queue writer to send control message to
         downstream downstream queue reader.
        """
        pass

    @abstractmethod
    def on_writer_message(self, buffer: bytes):
        """Called by downstream queue reader to send notify message to
        upstream queue writer.
        """
        pass

    @abstractmethod
    def on_writer_message_sync(self, buffer: bytes):
        """Called by downstream queue reader to send control message to
        upstream queue writer.
        """
        pass

    @abstractmethod
    def health_check(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def shutdown_without_reconstruction(self):
        pass

    @abstractmethod
    def notify_checkpoint_timeout(self, checkpoint_id_bytes):
        pass

    @abstractmethod
    def commit(self, barrier_bytes):
        pass

    @abstractmethod
    def clear_expired_cp(self, state_checkpoint_id_bytes,
                         queue_checkpoint_id_bytes,
                         partial_checkpoint_id_bytes):
        pass

    @abstractmethod
    def clear_partial_checkpoint(self, checkpoint_id_bytes,
                                 partial_checkpoint_id_bytes):
        pass

    @abstractmethod
    def fetch_metrics(self):
        pass

    @abstractmethod
    def check_if_need_rollback(self):
        pass

    @abstractmethod
    def request_rollback(self, exception_msg="Python exception."):
        pass

    @abstractmethod
    def resume(self):
        pass

    @abstractmethod
    def inject_exception(self, exception_injector_bytes):
        pass

    @abstractmethod
    def broadcast_partial_barrier(self, partial_barrier_bytes):
        pass

    @abstractmethod
    def fetch_profiling_infos(self):
        pass

    @abstractmethod
    def insert_control_message(self, message):
        pass

    @abstractmethod
    def rescale_rollback(self, paritial_checkpoint_id_bytes):
        pass


renamed_func_list = list(
    filter(
        lambda func_name: func_name != "set_annotated_methods",
        map(lambda x: x[0], inspect.getmembers(TideHandler,
                                               inspect.isfunction))))


def compatible(*args, **kwargs):
    def decorator(cls):
        ray_modified = hasattr(cls, "__ray_actor_class__")
        ray_actor_class_instance = cls
        if ray_modified:
            cls = getattr(cls, "__ray_actor_class__")
        for func_name in renamed_func_list:
            func = getattr(cls, func_name, None)
            if func:
                func.__name__ = generate_template.format(func.__name__)
                setattr(cls, func.__name__, func)
        return ray_actor_class_instance if ray_modified else cls

    return decorator(args[0])
