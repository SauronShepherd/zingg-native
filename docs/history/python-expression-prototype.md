# Python expression prototype

The pre-remediation implementation is preserved as a semantic and execution
feasibility reference only. It builds Spark expressions and phase orchestration
in Python and has no shared Scala core, Classic gateway, or Spark Connect server
plugin. It is therefore **not architecture compliant** and is not a release
candidate.

The repository baseline commit `e18799ea9350dcaa450c5c9b8afe99abe2bc82a0`
and tag `prototype-python-expressions-0.1.5` preserve the audited prototype
files alongside the remediation scaffolding. The prototype must not be used as
evidence for Databricks support of the target architecture.

## Scope

- Useful for comparing expression semantics and Photon plan behavior.
- Not a supported transport implementation.
- Not evidence of Zingg 0.7 workflow parity.
