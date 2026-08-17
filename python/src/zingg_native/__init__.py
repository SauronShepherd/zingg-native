"""Explicit PySpark facade for native-friendly Zingg operations."""

import socketserver

# PySpark 4.1's Windows client imports this Unix-only symbol during module
# initialization.  The Classic and Connect clients use TCP on Windows, so
# the standard TCP server is the compatible fallback for the installed wheel.
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer  # type: ignore[attr-defined]

from .zingg import Zingg
from .similarity import exact, exact_similarity, jaccard_similarity, jaro_similarity
from .runtime import RuntimeInfo, detect_runtime
from .config import NativeConfig
from .errors import ZinggNativeError, BackendUnavailableError, UnsupportedOperationError

__all__ = ["Zingg", "exact", "exact_similarity", "jaccard_similarity", "jaro_similarity", "RuntimeInfo", "detect_runtime", "NativeConfig", "ZinggNativeError", "BackendUnavailableError", "UnsupportedOperationError"]
