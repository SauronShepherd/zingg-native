"""Thin control surface for the transparent Zingg 0.7 native adapter."""

import socketserver

# PySpark 4.x on Windows may import this Unix-only symbol during initialization.
# Classic/Connect clients use TCP there, so this compatibility alias is harmless.
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer  # type: ignore[attr-defined]

from .adapter import activate
from .config import NativeConfig
from .errors import BackendUnavailableError, UnsupportedOperationError, ZinggNativeError
from .runtime import RuntimeInfo, detect_runtime
from .zingg import NativeAdapter, Zingg

__all__ = [
    "Zingg",
    "NativeAdapter",
    "activate",
    "RuntimeInfo",
    "detect_runtime",
    "NativeConfig",
    "ZinggNativeError",
    "BackendUnavailableError",
    "UnsupportedOperationError",
]
