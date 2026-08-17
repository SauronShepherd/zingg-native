"""Explicit PySpark facade for native-friendly Zingg operations."""

import socketserver

# PySpark 4.1's Windows client imports this Unix-only symbol during module
# initialization.  The Classic and Connect clients use TCP on Windows, so
# the standard TCP server is the compatible fallback for the installed wheel.
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer  # type: ignore[attr-defined]

from .config import NativeConfig
from .errors import BackendUnavailableError, UnsupportedOperationError, ZinggNativeError
from .runtime import RuntimeInfo, detect_runtime
from .similarity import exact, exact_similarity, jaccard_similarity, jaro_similarity
from .zingg import Zingg

__all__ = ["Zingg", "exact", "exact_similarity", "jaccard_similarity", "jaro_similarity", "RuntimeInfo", "detect_runtime", "NativeConfig", "ZinggNativeError", "BackendUnavailableError", "UnsupportedOperationError"]
