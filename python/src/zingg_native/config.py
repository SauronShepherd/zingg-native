"""Native execution policy shared by Classic/Py4J and Spark Connect clients."""
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class NativeConfig:
    mode: str = field(default_factory=lambda: os.getenv("ZINGG_NATIVE_MODE", "STRICT").upper())
    protocol_version: str = "1"
    operation_timeout_seconds: int = 3600
    disabled_rules: tuple[str, ...] = field(default_factory=lambda: tuple(filter(None,(x.strip() for x in os.getenv("ZINGG_NATIVE_DISABLED_RULES", "").split(",")))))

    def __post_init__(self) -> None:
        if self.mode not in {"OFF", "AUDIT", "REWRITE", "STRICT"}:
            raise ValueError("mode must be OFF, AUDIT, REWRITE, or STRICT")
        if self.operation_timeout_seconds <= 0:
            raise ValueError("operation_timeout_seconds must be positive")

    @property
    def rewrites(self) -> bool:
        return self.mode in {"REWRITE", "STRICT"}

    @property
    def audits(self) -> bool:
        return self.mode in {"AUDIT", "REWRITE", "STRICT"}
