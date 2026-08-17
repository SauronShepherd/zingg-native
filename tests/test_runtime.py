from zingg_native.runtime import detect_runtime
from zingg_native.backend.base import resolve_backend


class ConnectSession:
    version = "4.1.0"

    class conf:
        @staticmethod
        def get(key, default=""):
            return default


def test_connect_session_type_is_detected_when_conf_is_unavailable():
    runtime = detect_runtime(ConnectSession())
    assert runtime.api_mode == "connect"
    assert runtime.spark_version == "4.1.0"


def test_backend_autodetection_selects_connect_for_connect_session():
    class Conf:
        @staticmethod
        def get(key, default=""):
            if key == "zingg.native.connect.plugin.loaded":
                return "true"
            return default

    class Spark:
        version = "4.1.0"
        conf = Conf()

    Spark.__module__ = "pyspark.sql.connect.session"
    backend = resolve_backend(Spark())
    assert backend.name == "connect-plugin"
