from zingg_native.runtime import detect_runtime


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
