import kubernetes_operator
from kubernetes_operator.config.config_exception import ConfigException

_configured = False
_core_api = None
_auth_api = None
_extensions_beta_api = None


def _load_config():
    global _configured
    if _configured:
        return
    try:
        kubernetes_operator.config.load_incluster_config()
    except ConfigException:
        kubernetes_operator.config.load_kube_config()
    _configured = True


def core_api():
    global _core_api
    if _core_api is None:
        _load_config()
        _core_api = kubernetes_operator.client.CoreV1Api()

    return _core_api


def auth_api():
    global _auth_api
    if _auth_api is None:
        _load_config()
        _auth_api = kubernetes_operator.client.RbacAuthorizationV1Api()

    return _auth_api


def extensions_beta_api():
    global _extensions_beta_api
    if _extensions_beta_api is None:
        _load_config()
        _extensions_beta_api = kubernetes_operator.client.ExtensionsV1beta1Api()

    return _extensions_beta_api


log_prefix = "KubernetesOperatorNodeProvider: "
