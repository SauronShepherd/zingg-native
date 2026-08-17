"""Print deterministic SHA-256 checksums for the release artifacts."""

from __future__ import annotations

import hashlib
from pathlib import Path


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    artifacts = [
        next(root.glob("core/target/zingg-native-core_2.13-*.jar")),
        next(root.glob("connect/target/zingg-native-connect_2.13-*.jar")),
        sorted(root.glob("dist/zingg_native-*.whl"))[-1],
    ]
    for artifact in artifacts:
        print(f"{digest(artifact)}  {artifact.relative_to(root).as_posix()}")


if __name__ == "__main__":
    main()
