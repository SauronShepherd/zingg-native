# Python expression prototype

The pre-remediation implementation is preserved as a semantic and execution
feasibility reference only. It builds Spark expressions and phase orchestration
in Python and has no shared Scala core, Classic gateway, or Spark Connect server
plugin. It is therefore **not architecture compliant** and is not a release
candidate.

The exact preservation commit is recorded here after the repository baseline is
committed. The prototype must not be used as evidence for Databricks support of
the target architecture.

## Scope

- Useful for comparing expression semantics and Photon plan behavior.
- Not a supported transport implementation.
- Not evidence of Zingg 0.7 workflow parity.

