"""Stable public exception types."""


class ZinggNativeError(RuntimeError):
    """Base class for actionable library failures."""


class BackendUnavailableError(ZinggNativeError):
    """The requested transport/core is not installed or supported."""


class UnsupportedOperationError(ZinggNativeError):
    """The operation is not certified in the selected mode."""

