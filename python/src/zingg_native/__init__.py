"""Thin control surface for the transparent Zingg 0.7 native adapter."""

import socketserver

# PySpark 4.x on Windows may import this Unix-only symbol during initialization.
# Classic/Connect clients use TCP there, so this compatibility alias is harmless.
if not hasattr(socketserver, "UnixStreamServer"):
    setattr(socketserver, "UnixStreamServer", socketserver.TCPServer)

from .adapter import activate
from .config import NativeConfig
from .errors import BackendUnavailableError, UnsupportedOperationError, ZinggNativeError
from .runtime import RuntimeInfo, detect_runtime
from .zingg import NativeAdapter, Zingg

__all__ = [
    "BackendUnavailableError",
    "NativeAdapter",
    "NativeConfig",
    "RuntimeInfo",
    "UnsupportedOperationError",
    "Zingg",
    "ZinggNativeError",
    "activate",
    "detect_runtime",
]
