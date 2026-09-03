# Closeout contracts

This document records the security, correctness, compatibility, and deployment contracts that must remain true for the Zingg Native release line.

## Supported managed environments

The supported managed targets are Databricks Dedicated with Photon and Databricks Serverless. The production JVM line is Java 17, Scala 2.13, and Spark 4.x. Spark 3 and Scala 2.12 compatibility shims are intentionally outside this release contract.

## Required `--zinggDir`

Native model and materialization state requires an explicit `--zinggDir`. Launchers must fail before writing native materialization data when it is omitted. There is no implicit development Volume or local fallback directory.

Each invocation derives a run-scoped root beneath `.native-transient` using the native run ID. Cleanup and graph materialization operate only beneath that validated run root.

## Transient cleanup containment

Cleanup is idempotent per run root. The cleanup root is canonicalized, UUID-scoped, and must remain beneath `.native-transient`. Symbolic-link roots, symbolic-link ancestors, and symbolic-link descendants are rejected. Canonical descendants must remain beneath the canonical run root, and traversal must not follow links.

A rejected cleanup must never modify files outside the scoped run root.

## Graph materialization containment

An explicit graph materialization path is valid only when a run root exists and the configured path equals the run root or is a true descendant of it. Prefix lookalikes such as `/run/a` and `/run/ab` are not equivalent. `..` traversal, outside absolute paths, and symlink escapes are invalid.

Only the validated scoped graph path may be passed to cleanup.

## Connected-components convergence

Connected components is fail-closed. `maxIterations` must be positive. If convergence is not reached before the effective iteration cap, both `STRICT` and `REWRITE` execution fail rather than publishing or returning partial component labels. Successful convergence preserves the complete component result.

Diagnostics may identify the run, phase, strategy, and iteration cap, but must not log row values or sensitive data.

## Pinned upstream semantic reference

Compatibility is evaluated against the pinned Zingg 0.7.0 source reference recorded under `reference/`. Validation prepares a clean detached sparse checkout at the pinned commit before running inventory, source-context, and differential semantic checks. Production modifications belong in `integration/zingg-0.7.0-overlay`, not in the pinned reference checkout.

## Jaro / Jaro-Winkler compatibility

`JaroWinklerFunction` is a compatibility boundary, not permission to silently change semantics based on its name. Native behavior must track the pinned upstream Zingg 0.7.0/SecondString behavior. Any future semantic correction requires coordinated updates to the pinned reference contract, native rewrite, differential fixtures, compatibility version, and release notes.

## Rewrite and observability contract

Production rewrites must be registered, individually disable-able, and observable. `STRICT` must reject unknown or unmapped operations. No path may claim native execution after falling back. Classic/Py4J and Spark Connect use the same rewrite registry and semantic contracts. Photon attribution requires runtime evidence and is never inferred only from configuration.

Diagnostics must not log row values or sensitive data.

## Local verification

A release candidate should be validated from a clean Linux/WSL checkout with the declared Python development dependencies, Python 3.10 and 3.12 checks, the Java 17 Maven production build, PowerShell/Python contract scripts, JVM/filesystem tests, `git diff --check`, and a final staged-file review. The GitHub closeout is complete only after the final `main` commit has green CI and CodeQL and repository protection/security settings have been verified in GitHub itself.
