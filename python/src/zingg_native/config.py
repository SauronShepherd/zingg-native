"""Validated, transport-neutral configuration."""

from dataclasses import dataclass


@dataclass(frozen=True)
class NativeConfig:
    mode: str = "SAFE"
    protocol_version: str = "1"
    operation_timeout_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.mode not in {"SAFE", "EXPERIMENTAL"}:
            raise ValueError("mode must be SAFE or EXPERIMENTAL")
        if self.operation_timeout_seconds <= 0:
            raise ValueError("operation_timeout_seconds must be positive")
