"""Activation/diagnostics for the transparent native adapter.

Normal patched Zingg jobs do not need to import this module. It is useful for
Python launchers that want to select policy before constructing a Classic JVM
Zingg job or a Spark Connect expression helper.
"""
from __future__ import annotations

import os
from dataclasses import asdict
from typing import Any

from .config import NativeConfig
from .errors import BackendUnavailableError
from .runtime import detect_runtime


def activate(spark: Any, config: NativeConfig | None = None, run_id: str | None = None) -> dict[str, Any]:
    cfg=config or NativeConfig()
    os.environ["ZINGG_NATIVE_MODE"]=cfg.mode
    if cfg.disabled_rules:
        os.environ["ZINGG_NATIVE_DISABLED_RULES"]=",".join(cfg.disabled_rules)
    if run_id:
        os.environ["ZINGG_NATIVE_RUN_ID"]=run_id
    # Classic can propagate policy to the in-process JVM without Spark conf.
    # Connect deliberately has no private JVM handle. Databricks Serverless JAR
    # jobs receive these settings through DatabricksZinggMain launcher arguments,
    # which are translated to JVM properties before the real Zingg main starts.
    # The environment values above are only a local/Dedicated convenience.
    info=detect_runtime(spark)
    if info.api_mode=="classic":
        try:
            from .backend.classic import set_native_properties
            set_native_properties(spark, cfg.mode, cfg.disabled_rules, run_id)
        except Exception as exc:
            if cfg.mode == "STRICT":
                raise BackendUnavailableError(
                    "Unable to activate STRICT native mode on the Classic JVM transport"
                ) from exc
            # AUDIT/REWRITE still return diagnostics so callers can decide
            # whether to continue when the optional JVM bridge is unavailable.
            return {
                "config": asdict(cfg),
                "runtime": asdict(info),
                "run_id": run_id,
                "activation_error": str(exc),
            }
    return {"config":asdict(cfg),"runtime":asdict(info),"run_id":run_id}
