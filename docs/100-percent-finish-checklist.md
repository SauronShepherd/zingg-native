# Zingg-Native 100% Finish Checklist

This is the complete closeout checklist for the current security, correctness, CI, repository-governance, and compatibility work.

## Definition of done

- [ ] `main` CI is green for the final commit.
- [ ] CodeQL is green for the final commit.
- [ ] All local Linux/WSL validations pass.
- [ ] JVM production artifacts build successfully with Java 17, Scala 2.13, and Spark 4.x.
- [ ] Python validation passes on Python 3.10 and 3.12.
- [ ] All security findings ZN-001, ZN-002, and ZN-003 are implemented and tested.
- [ ] Repository protection settings are verified on GitHub, not only represented by files.
- [ ] The final commit is pushed to `main` and the final CI run is recorded.
- [ ] No unrelated generated files or experimental work are included in the final change set.

## 1. Immediate CI blockers

### 1.1 Python 3.10 and 3.12 mypy failure

- [ ] Inspect the complete mypy output for jobs `python (3.10)` and `python (3.12)` in the latest run.
- [ ] Reproduce with the exact CI interpreter and installed dependency versions.
- [ ] Confirm whether the `socketserver.UnixStreamServer` compatibility alias is the only diagnostic.
- [ ] Use a narrow, version-compatible typing solution; do not weaken mypy globally.
- [ ] Keep the runtime compatibility behavior unchanged.
- [ ] Run:
  ```bash
  python -m mypy python/src
  python -m ruff check python/src tests
  python -m pytest -q
  ```
- [ ] Verify both Python matrix jobs pass in GitHub Actions.

### 1.2 JVM production artifact failure

- [ ] Obtain the complete Maven output for the failed `jvm` job.
- [ ] Identify the exact failing command after compilation; do not treat the job-level failure as sufficient diagnosis.
- [ ] Reproduce in WSL with Java 17 and Maven:
  ```bash
  mvn -B -Dmaven.test.skip=true -Pdatabricks-serverless-env5 package -pl serverless-launcher -am
  ```
- [ ] Fix only the underlying source, build, or validation failure.
- [ ] Confirm the following commands pass:
  ```bash
  pwsh ./scripts/check-serverless-core.ps1
  pwsh ./scripts/check-serverless-dependencies.ps1
  pwsh ./scripts/check-artifacts.ps1
  pwsh ./scripts/check-phase-contract.ps1
  pwsh ./scripts/check-upstream-native-patch.ps1
  pwsh ./scripts/check-source-context.ps1
  python3 scripts/check-source-boundaries.py
  python3 scripts/check-serverless-bytecode.py core/target/*.jar serverless-launcher/target/*.jar dist/*.jar
  python3 scripts/check-release-metadata.py
  python3 scripts/verify-artifacts.py
  ```
- [ ] Verify the Spark 4.0/default profile and Spark 4.1 profile as required by the project matrix.

### 1.3 CI matrix completion

- [ ] Re-run the complete workflow after both blockers are fixed.
- [ ] Confirm Python 3.10 passes.
- [ ] Confirm Python 3.12 passes.
- [ ] Confirm JVM passes.
- [ ] Confirm semantic-reference passes.
- [ ] Confirm CodeQL passes.
- [ ] Confirm no matrix jobs are cancelled because of an upstream failure.
- [ ] Record the final successful run URL in the closeout PR or release notes.

## 2. ZN-001: connected-components correctness

- [x] Make non-convergence fail closed in every execution mode.
- [x] Prevent `REWRITE` from publishing partial labels.
- [x] Preserve successful convergence behavior.
- [ ] Add or verify a focused long-path graph test whose diameter exceeds `maxIterations`.
- [ ] Assert `STRICT` throws on non-convergence.
- [ ] Assert `REWRITE` also throws on non-convergence.
- [ ] Assert no partial labels are returned in either mode.
- [ ] Add a successful-convergence test with a path within the iteration cap.
- [ ] Add a boundary test for `maxIterations = 0` or the minimum supported value.
- [ ] Add a test for a disconnected graph with multiple components.
- [ ] Add a test for repeated edges and self-loops if those are valid inputs.
- [ ] Verify diagnostics identify the run, phase, iteration cap, and fail-closed outcome without logging row values.
- [ ] Document the convergence contract and iteration-cap behavior.
- [ ] Consider pointer jumping/hooking only as a separate performance follow-up; do not weaken correctness to improve convergence.

## 3. Materialization cleanup security

- [x] Remove the JVM-global one-shot cleanup guard.
- [x] Make cleanup idempotent per run root.
- [x] Canonicalize the cleanup root.
- [x] Reject symbolic-link roots.
- [x] Reject symbolic-link ancestors.
- [x] Reject symbolic-link descendants.
- [x] Verify canonical descendants remain under the canonical run root.
- [x] Use a non-link-following file-tree visitor.
- [ ] Add an adversarial test where a descendant is replaced with a symlink during traversal, if the test platform supports safe race simulation.
- [ ] Add a test for a symlinked parent directory.
- [ ] Add a test for a symlinked file pointing outside the run root.
- [ ] Add a test that an outside sentinel file remains untouched after every rejection path.
- [ ] Add a test for a missing root and verify retry behavior after a failed cleanup.
- [ ] Add a test cleaning two different UUID roots in the same JVM.
- [ ] Verify cleanup rejects malformed UUIDs, non-scoped paths, empty paths, and paths outside `.native-transient`.
- [ ] Verify cleanup does not follow links when checking existence.
- [ ] Run the filesystem tests on Linux/WSL and Windows where supported.

## 4. ZN-002: unmanaged materialization fallback

- [x] Remove the hard-coded development Volume fallback.
- [x] Require an explicit `--zinggDir` for model/materialization state.
- [x] Add a contract test for the missing-directory argument.
- [ ] Search the entire repository for the old fallback path and remove every remaining production reference.
- [ ] Search generated documentation and examples for implicit fallback claims.
- [ ] Verify Dedicated + Photon supplies the directory correctly.
- [ ] Verify Serverless supplies the directory correctly.
- [ ] Verify an omitted directory fails with an actionable error before writing data.
- [ ] Verify no test silently creates a local fallback directory.
- [ ] Document the required storage contract for launchers and deployment configurations.

## 5. ZN-003: graph materialization containment

- [x] Require explicit graph materialization paths to have a run root.
- [x] Require configured paths to equal or remain beneath the scoped run root.
- [x] Scope the materialization path by run ID.
- [x] Add a source contract test for outside-root rejection.
- [ ] Add runtime tests for an absolute outside path.
- [ ] Add runtime tests for `..` traversal.
- [ ] Add runtime tests for path-prefix confusion, such as `/run/a` versus `/run/ab`.
- [ ] Add runtime tests for symlinked graph directories.
- [ ] Add an outside sentinel and verify it is never created, written, or deleted.
- [ ] Verify cleanup receives only the validated scoped path.
- [ ] Document the graph materialization path contract.

## 6. Jaro/Jaro-Winkler compatibility contract

- [ ] Confirm the pinned upstream Zingg 0.7.0 implementation and its actual SecondString semantics.
- [ ] Preserve the explicit semantic-boundary comment in the native rewrite rule.
- [ ] Add a differential test against the pinned upstream behavior for representative pairs.
- [ ] Include equal strings, empty strings, transpositions, prefix matches, and short strings.
- [ ] Add a regression fixture proving the native rule currently matches upstream behavior, including the named `JaroWinklerFunction` boundary.
- [ ] Version the differential fixture/contract with the upstream semantic version.
- [ ] Define the required change procedure if upstream corrects Jaro-Winkler: update reference, native rule, fixtures, compatibility version, and release notes together.
- [ ] Ensure the native implementation is not silently changed merely because the operation name says “JaroWinkler.”

## 7. Source-context and pinned-reference integrity

- [x] Pin the upstream Zingg 0.7.0 commit.
- [x] Verify the reference checkout is clean and detached at the pinned commit.
- [x] Make the reference digest platform-stable using Git’s canonical tree listing.
- [x] Keep the overlay digest validation.
- [ ] Run source-context validation in WSL/Linux.
- [ ] Run source-context validation in Windows PowerShell.
- [ ] Verify sparse checkout includes every required upstream source directory.
- [ ] Verify overlay files all have corresponding pinned upstream context.
- [ ] Verify no production source is added directly to `reference/upstream-zingg`.
- [ ] Verify production changes belong in `integration/zingg-0.7.0-overlay`.

## 8. Python quality and compatibility

- [ ] Pin or constrain formatter/linter/type-checker versions sufficiently for reproducible CI.
- [ ] Run Ruff lint and formatting checks.
- [ ] Run mypy on Python 3.10 and 3.12.
- [ ] Run Python bytecode compilation.
- [ ] Run the full pytest suite.
- [ ] Run architecture-boundary checks.
- [ ] Run upstream operation inventory checks.
- [ ] Run phase-contract checks.
- [ ] Run Photon-evidence checks.
- [ ] Confirm no Python UDF or Scala UDF is introduced into the production path.
- [ ] Confirm imports remain compatible with Python 3.10.

## 9. JVM and managed-runtime quality

- [ ] Build with Java 17.
- [ ] Compile with Scala 2.13.
- [ ] Build the Spark 4 Serverless artifact.
- [ ] Build/check the Dedicated + Photon artifact path.
- [ ] Verify Spark 4.0 and Spark 4.1 profiles as configured.
- [ ] Verify Databricks Connect dependencies remain transport-specific and do not introduce a separate semantic implementation.
- [ ] Verify no Spark 3 or Scala 2.12 compatibility shim is added.
- [ ] Verify no Catalyst API, planner extension, SparkSessionExtension, or SparkContext dependency is introduced into the common/Serverless artifact.
- [ ] Verify bytecode checks reject prohibited APIs.
- [ ] Verify artifact metadata and checksums.
- [ ] Verify assembly does not contain duplicate or forbidden dependencies.

## 10. Rewrite registry and observability

- [ ] Confirm every production rewrite is registered and individually disable-able.
- [ ] Confirm every rewrite emits observable evidence.
- [ ] Confirm `STRICT` rejects unknown or unmapped operations.
- [ ] Confirm no path claims native execution after falling back.
- [ ] Confirm Classic/Py4J and Spark Connect use the same rewrite registry and semantics.
- [ ] Confirm diagnostics never log row values or sensitive data.
- [ ] Confirm graph non-convergence is observable as a failed operation.
- [ ] Confirm Photon claims require runtime evidence and are not inferred from configuration.

## 11. GitHub repository governance

- [x] Add `CONTRIBUTING.md`.
- [x] Add `SECURITY.md`.
- [x] Add a single-owner `CODEOWNERS` rule for `@SauronShepherd`.
- [x] Add issue forms.
- [x] Add pull-request template.
- [x] Add Dependabot configuration.
- [x] Add CodeQL workflow.
- [x] Add dependency-review workflow.
- [x] Add fork-policy workflow/documentation.
- [ ] Enable branch protection or rulesets on `main` in GitHub repository settings.
- [ ] Require pull requests before merging.
- [ ] Require the final CI status checks before merging.
- [ ] Require CodeQL before merging.
- [ ] Require dependency review before merging where supported.
- [ ] Require the single code owner approval.
- [ ] Disable direct pushes to `main`, including administrator bypass if organizational policy permits.
- [ ] Disable force pushes and branch deletion for `main`.
- [ ] Require conversation resolution.
- [ ] Require branches to be up to date before merging if practical.
- [ ] Disable merge commits if the project’s chosen history policy requires squash/rebase.
- [ ] Confirm fork-based pull requests work for external contributors.
- [ ] Confirm workflows from forks receive no write-capable secrets.
- [ ] Confirm Dependabot PRs can pass the required checks.
- [ ] Confirm CODEOWNERS is recognized by GitHub and the owner is notified.
- [ ] Confirm the repository’s default branch is `main`.
- [ ] Confirm Actions permissions are least-privilege and workflow tokens default to read-only.
- [ ] Confirm security alerts, secret scanning, and push protection are enabled where available.

## 12. Documentation and release hygiene

- [ ] Document supported environments: Databricks Dedicated + Photon and Databricks Serverless.
- [ ] Document required `--zinggDir` behavior.
- [ ] Document transient-root and graph-path containment rules.
- [ ] Document fail-closed connected-components behavior.
- [ ] Document the pinned upstream semantic reference.
- [ ] Document Jaro/Jaro-Winkler compatibility and versioning.
- [ ] Document how to run all local checks in WSL.
- [ ] Remove stale statements that describe old fallback behavior.
- [ ] Remove stale statements that describe Spark 3/Scala 2.12 support.
- [ ] Remove generated artifacts, temporary parquet files, reports, and local bundle summaries unless intentionally tracked.
- [ ] Review all staged files before the final commit.
- [ ] Confirm no credentials, tokens, private URLs, or local-machine paths are committed.
- [ ] Update changelog/release notes with security and correctness fixes.

## 13. Final verification sequence

- [ ] Start from a clean WSL checkout of the final commit.
- [ ] Install the project’s declared Python development dependencies.
- [ ] Run the complete Python matrix locally where possible.
- [ ] Run the complete Maven production build.
- [ ] Run all PowerShell and Python contract scripts.
- [ ] Run all JVM tests and filesystem adversarial tests.
- [ ] Review `git diff --check`.
- [ ] Review `git status --short`.
- [ ] Review `git diff --cached --stat` before committing.
- [ ] Commit only intended files.
- [ ] Push the final commit to `main`.
- [ ] Wait for all required GitHub checks to complete.
- [ ] Confirm every required check is green.
- [ ] Confirm no cancelled matrix job remains.
- [ ] Confirm branch protection/ruleset settings are active.
- [ ] Record the final commit SHA and successful Actions URLs.
- [ ] Mark this checklist complete only after all unchecked items that apply are resolved or explicitly documented as not applicable.

## Current known state at checklist creation

- Latest pushed fix: `141da65`.
- Previous CI status: semantic-reference and CodeQL passed; Python mypy and JVM artifact build failed.
- WSL is available, but the WSL Python environment currently lacks the project’s Ruff dependency.
- Existing unrelated worktree changes must not be included accidentally in follow-up commits.
