"""Verify the release artifact shape without requiring a Spark runtime."""

from __future__ import annotations

import glob
import json
import sys
import zipfile
from pathlib import Path


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    core = next(root.glob("core/target/zingg-native-core_2.13-*.jar"), None)
    connect = next(root.glob("connect/target/zingg-native-connect_2.13-*.jar"), None)
    wheels = sorted(root.glob("dist/zingg_native-*.whl"))
    if not core or not connect or not wheels:
        fail("release artifacts are missing; run the JVM and wheel builds first")

    with zipfile.ZipFile(core) as jar:
        names = set(jar.namelist())
        required = {
            "ai/zingg/native/Core.class",
            "ai/zingg/native/gateway/ClassicGateway.class",
            "zingg-native-capabilities.json",
        }
        missing = sorted(required - names)
        if missing:
            fail(f"core JAR missing: {', '.join(missing)}")
        capabilities = json.loads(jar.read("zingg-native-capabilities.json"))
        if capabilities["sparkLine"] != "4.1" or capabilities["scalaBinaryVersion"] != "2.13":
            fail("core capability manifest is not pinned to Spark 4.1 / Scala 2.13")

    with zipfile.ZipFile(connect) as jar:
        names = set(jar.namelist())
        required = {"ai/zingg/native/connect/ZinggNativeExpressionPlugin.class"}
        missing = sorted(required - names)
        if missing:
            fail(f"Connect JAR missing: {', '.join(missing)}")

    with zipfile.ZipFile(wheels[-1]) as wheel:
        names = set(wheel.namelist())
        required = {
            "zingg_native/__init__.py",
            "zingg_native/zingg.py",
            "zingg_native/backend/classic.py",
            "zingg_native/backend/connect.py",
        }
        missing = sorted(required - names)
        if missing:
            fail(f"wheel missing: {', '.join(missing)}")
        if any("pyspark" in name or "spark-core" in name for name in names):
            fail("wheel must not vendor the Spark runtime")

    print(f"Artifacts valid: {core.name}, {connect.name}, {wheels[-1].name}")


if __name__ == "__main__":
    main()
