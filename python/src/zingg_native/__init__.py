"""Explicit PySpark facade for native-friendly Zingg operations."""

from .zingg import Zingg
from .similarity import exact, exact_similarity, jaccard_similarity, jaro_similarity
from .runtime import RuntimeInfo, detect_runtime
from .config import NativeConfig
from .errors import ZinggNativeError, BackendUnavailableError, UnsupportedOperationError

__all__ = ["Zingg", "exact", "exact_similarity", "jaccard_similarity", "jaro_similarity", "RuntimeInfo", "detect_runtime", "NativeConfig", "ZinggNativeError", "BackendUnavailableError", "UnsupportedOperationError"]
