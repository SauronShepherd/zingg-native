# Manual test catalog

This document is the exhaustive manual-test checklist for `zingg-native`.
It is intentionally broader than the automated unit, contract, static, and
Databricks probes already present in the repository. A checkbox in this file
does not mean that the test has passed; record the result and evidence for each
execution. The IDs are grouped by subject area, so the numeric range is a
stable reference namespace rather than a test count.

The catalog is written for the supported product scope:

- Databricks Dedicated + Photon;
- Databricks Serverless environment 5;
- Spark 4.x, Scala 2.13, and Java 17;
- Classic/Py4J and managed Spark Connect as transport variants over the same
  rewrite registry;
- the ordinary Zingg 0.7.0 application and phase contract.

Spark 3.x, Scala 2.12, a custom Connect planner, Catalyst extensions, Python
UDFs, Scala UDFs, and a second `zingg_native` entity-resolution API are not
supported acceptance paths. They should be negative compatibility checks, not
additional implementation targets.

## How to use this catalog

For every test, record at least:

- date, commit, release ID, and the exact JAR checksums;
- target (`dedicated-photon` or `serverless-env5`), Spark version, Scala
  version, Java version, transport, and cluster/job ID;
- native mode, run ID, disabled rules, plan-guard setting, and the ordinary
  Zingg phase or probe;
- input fixture/configuration, row count, schema, output paths, and output
  fingerprints;
- Databricks run ID, task run ID, query IDs, driver logs, and relevant query
  profile evidence;
- expected result, observed result, and a link to the retained evidence.

Use `PASS` only when the expected result and the relevant evidence are both
present. Use `BLOCKED` for an environment or credential limitation, and do not
turn a blocked Dedicated test into a Serverless claim. Use `FAIL` when the
implementation behaves incorrectly, silently falls back, loses data, or emits
an unsupported claim.

## Test fixtures and reference rules

Use the smallest fixture that isolates the behavior first, then repeat the
successful case with the production-shaped fixture.

| Fixture | Purpose |
|---|---|
| `scripts/databricks-config-minimal-parquet.json` and its preparation script | Minimal ordinary Zingg input and persistence smoke path |
| `scripts/databricks-config-four-feature-exact.json` | Exact and multi-feature training smoke path |
| `scripts/databricks-config-four-feature-marked.json` | Marked data, train, and model boundary path |
| `scripts/databricks-config-one-fuzzy.json` | Isolated Jaro/affine-gap training path |
| `scripts/databricks-config-full-parquet.json` | Production-shaped 20-feature/full-model path |
| `scripts/databricks-config-tiny-marked.json` | Fast phase-by-phase `train`, `match`, `link`, `label`, and `updateLabel` checks |
| `reference/upstream-zingg` | Pinned Zingg 0.7.0 semantic oracle; never edit it |
| `docs/evidence/databricks-serverless-v5.json` | Existing runtime evidence and known limitations |
| `resources/serverless-*.yml` | Reusable Serverless validation jobs and probe entry points |

The current resource files map to the catalog as follows. This inventory is
also a quick way to detect a new probe that has not yet received a manual
acceptance case.

| Resource | Primary manual coverage |
|---|---|
| `dedicated-photon-zingg-native.yml` | MT-388, MT-440 |
| `serverless-zingg-native.yml` | MT-133, MT-185, MT-300, MT-302–MT-314 |
| `serverless-blocking-differential-probe.yml` | MT-224–MT-229 |
| `serverless-datagen-stress.yml` | MT-349 |
| `serverless-date-array-differential-probe.yml` | MT-168–MT-169 |
| `serverless-failure-probes.yml` | MT-062–MT-066 |
| `serverless-four-feature-probe.yml` | MT-247, MT-265, MT-306 |
| `serverless-four-feature-train-only.yml` | MT-306, MT-313 |
| `serverless-full-parquet-diagnostic.yml` | MT-306, MT-290 |
| `serverless-fuzzy-action-probe.yml` | MT-182–MT-183 |
| `serverless-fuzzy-action-scale-probe.yml` | MT-183, MT-405 |
| `serverless-fuzzy-rule-probes.yml` | MT-173–MT-175, MT-184 |
| `serverless-graph-benchmark.yml` | MT-348, MT-402–MT-403 |
| `serverless-graph-probe.yml` | MT-330–MT-347 |
| `serverless-hash-differential-probe.yml` | MT-210–MT-221 |
| `serverless-input-format-probe.yml` | MT-130–MT-146 |
| `serverless-ldbc-stress.yml` | MT-350, MT-408–MT-409 |
| `serverless-materialization-recovery.yml` | MT-360–MT-370 |
| `serverless-model-cross-job.yml` | MT-276–MT-279 |
| `serverless-model-failure-probe.yml` | MT-281–MT-285 |
| `serverless-model-parity-probe.yml` and `serverless-model-sparkml-oracle.yml` | MT-262–MT-275, MT-286–MT-288 |
| `serverless-model-semantic-probe.yml` | MT-260–MT-264 |
| `serverless-numeric-differential-probe.yml` | MT-164–MT-167 |
| `serverless-one-fuzzy-probe.yml` | MT-174–MT-175, MT-306 |
| `serverless-phase-failure-probe.yml` | MT-098, MT-315 |
| `serverless-production-validation.yml` | MT-266–MT-267, MT-307, MT-406, MT-410 |
| `serverless-row-id-probe.yml` | MT-144–MT-145 |
| `serverless-similarity-materialization-probe.yml` | MT-186–MT-187, MT-371 |
| `serverless-stopwords-differential-probe.yml` | MT-197–MT-201 |
| `serverless-twitter-stress.yml` | MT-350, MT-409 |
| `serverless-vector-probe.yml` | MT-240–MT-248 |

The oracle comparison must compare values, nulls, types, row counts, candidate
pairs, model metadata, and final partitions. Comparing only job success or a
single aggregate count is insufficient.

## 1. Repository, source, and release hygiene

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-001 | Working-tree inventory | Review `git status`, changed files, generated files, and release inputs before a test run. | User changes are preserved; credentials, temporary data, and generated outputs are not accidentally packaged. |
| MT-002 | Reference immutability | Verify that no command, patch, build, or release step edits `reference/upstream-zingg`. Compare its HEAD/checksum before and after the run. | The pinned semantic reference is unchanged. |
| MT-003 | Overlay ownership | Confirm that production integration changes are under `integration/zingg-0.7.0-overlay` and that the supplied Zingg assembly is external to this repository. | No hidden fork or working-tree mutation is used. |
| MT-004 | Phase contract | Run `pwsh -File scripts/check-phase-contract.ps1`. Inspect the contract for `findTrainingData`, `label`, `train`, `match`, `link`, and `updateLabel`. | The six phases are present, ordered, and mapped to `STRICT`/`--native-mode` activation. |
| MT-005 | Upstream inventory | Run `pwsh -File scripts/check-upstream-inventory.ps1` and inspect any mismatch against the pinned 0.7.0 source. | The native inventory is traceable to upstream operations; no unclassified reachable choke point is accepted. |
| MT-006 | Reference patch boundary | Run `pwsh -File scripts/check-reference.ps1` and inspect the result. | Reference content is available as an immutable input and is not treated as production source. |
| MT-007 | Source context | Run `pwsh -File scripts/check-source-context.ps1`. | Production source contains the expected package/module boundaries and no accidental legacy plugin dependency. |
| MT-008 | Production architecture scan | Run `pwsh -File scripts/check-architecture.ps1`. | Python remains control/transport-only except for the approved Classic bridge. |
| MT-009 | Source forbidden-boundary scan | Run `python scripts/check-source-boundaries.py`. | No production Catalyst, planner extension, SparkContext, GraphFrames compile dependency, cache, persist, or checkpoint call is found. |
| MT-010 | Phase and rule documentation consistency | Compare `native-operation-inventory.yaml`, `docs/rewrite-rules.md`, `docs/semantic-contracts.md`, and the registry in `RewriteArchitecture.scala`. | Every claimed rule is either implemented, explicitly unvalidated, or explicitly fail-closed/unreachable. |
| MT-011 | Historical code separation | Confirm that `reference/legacy-connect-plugin/` is not in the Maven reactor, dependency tree, or deployed JARs. | The archived Connect experiment is provenance only. |
| MT-012 | Documentation link review | Open every relative link in the README and `docs/`, including evidence links and runbook links. | Links resolve to current files; stale deleted checklists are not referenced. |

## 2. Build and artifact checks

Run each build in a clean output directory when possible. Do not use a local
successful compile as evidence of Databricks runtime compatibility.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-020 | Default core build | Run `pwsh -File scripts/build.ps1 -Target core`. | The public core artifact builds with Java 17 and Scala 2.13; no Serverless launcher is silently included. |
| MT-021 | Spark 4.0 public API build | Run `pwsh -File scripts/build.ps1 -Target spark40`. | Core compiles against Spark 4.0 with no Spark 3/Scala 2.12 shim. |
| MT-022 | Spark 4.1 public API build | Run `pwsh -File scripts/build.ps1 -Target spark41`. | Core compiles against Spark 4.1 with no Spark 3/Scala 2.12 shim. |
| MT-023 | Dedicated 17.3 build | Run `pwsh -File scripts/build.ps1 -Target dedicated17`. | Dedicated 17.3 artifact builds for Spark 4.x/Scala 2.13/Java 17. |
| MT-024 | Dedicated 18 LTS build | Run `pwsh -File scripts/build.ps1 -Target dedicated18`. | Dedicated 18 LTS artifact builds for Spark 4.x/Scala 2.13/Java 17. |
| MT-025 | Serverless build | Run `pwsh -File scripts/build.ps1 -Target serverless`. | Core and launcher build with Databricks Connect provided dependencies; Spark/Connect runtime classes are not bundled. |
| MT-026 | Legacy converter build | Run `pwsh -File scripts/build.ps1 -Target converter`. | The optional converter builds separately and is not a Serverless/common production dependency. |
| MT-027 | Optional wheel build | Run `pwsh -File scripts/build.ps1 -Target core -Wheel`. | The Python control surface packages without adding a Python semantic engine. |
| MT-028 | Artifact presence and metadata | Run `pwsh -File scripts/check-artifacts.ps1`, then inspect manifests and versions. | Expected core and launcher JARs exist with the expected `0.3.0-SNAPSHOT`/protocol metadata. |
| MT-029 | Artifact checksums | Run `python scripts/artifact-checksums.py` against the release set and retain the manifest. | Core, launcher, patched Zingg assembly, and metadata are identified as one release set. |
| MT-030 | Serverless bytecode boundary | Run `python scripts/check-serverless-bytecode.py` on core, launcher, and patched Zingg JARs. | Java class files are no newer than major 61; forbidden Catalyst, SparkContext, GraphFrames, RDD, custom Connect, and Scala 2.12 references are absent. |
| MT-031 | Serverless dependency boundary | Run `pwsh -File scripts/check-serverless-dependencies.ps1` and inspect the dependency tree. | No Spark distribution, malformed dbutils bridge, GraphFrames, or legacy Connect plugin is pulled into the common artifact. |
| MT-032 | Assembly validation | Run `python scripts/check-zingg-assembly.py dist/zingg-0.7.0-spark4-native.jar`. | The patched Zingg assembly contains the expected overlay and native provider seam. |
| MT-033 | Artifact verification | Run `python scripts/verify-artifacts.py` using the built core and launcher. | Checksums, required entries, and release metadata pass. |
| MT-034 | Capability manifest | Run `python scripts/generate-capabilities.py` or inspect the generated manifest. | Runtime evidence, unvalidated semantic parity, and Dedicated claims are distinguished; no Photon claim is inferred from configuration. |
| MT-035 | Reproducible patched assembly | Run `pwsh -File scripts/build-patched-zingg.ps1 -Output dist/zingg-0.7.0-spark4-native.jar` twice from the same inputs. | The assembly is reproducible or any documented timestamp difference is limited to approved metadata. |
| MT-036 | Clean release pipeline | Run `pwsh -File scripts/release-serverless.ps1 -ReleaseId <unique-id> -Profile sda` without `-Deploy` after authenticating. | Build, patch, boundary scan, fixture publication, immutable release publication, and bundle validation occur in that order. |
| MT-037 | Release checksum re-read | Inspect the remote release directory and compare its SHA-256 manifest with local files. | A changed or partially uploaded JAR is rejected. |
| MT-038 | Bundle validation | Run `databricks bundle validate -t serverless -p <profile>` and the corresponding Dedicated target. | Variables, dependencies, main classes, environment versions, and cluster settings resolve without a deployment. |
| MT-039 | Unsupported runtime negative build | Attempt a Spark 3.x or Scala 2.12 production build/configuration. | It is rejected or clearly unsupported; no compatibility shim is introduced. |

## 3. Native policy, rule registry, and fail-closed behavior

Classic/Dedicated supports all four policy modes. Serverless accepts only
`REWRITE` and `STRICT`; this is an intentional launcher contract.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-050 | Dedicated `OFF` | Run an ordinary Zingg phase with `-Dzingg.native.mode=OFF`; inspect plan and logs. | Upstream execution is preserved, native rewrites are not applied, and no native execution claim is emitted. |
| MT-051 | Dedicated `AUDIT` | Run a phase with `-Dzingg.native.mode=AUDIT`. | Legacy constructs are recorded, no rewrite is substituted, and the phase remains a legacy compatibility run. |
| MT-052 | Dedicated `REWRITE` | Run a supported operation with `-Dzingg.native.mode=REWRITE`. | Known operations are rewritten and observable; unsupported operations are not silently claimed native. |
| MT-053 | Dedicated `STRICT` default | Omit the mode on a native-enabled Dedicated run. | The default is `STRICT`, and an unknown operation fails closed before claiming native execution. |
| MT-054 | Serverless `REWRITE` | Submit a supported Serverless probe with `--native-mode REWRITE`. | The managed launcher starts and rewrites supported operations. |
| MT-055 | Serverless `STRICT` | Submit the same probe with `--native-mode STRICT`. | The same semantic registry is used and strict plan/rule failures are visible. |
| MT-056 | Serverless `OFF` rejection | Submit any Serverless job with `--native-mode OFF`. | The launcher rejects the mode before ordinary Zingg execution. |
| MT-057 | Serverless `AUDIT` rejection | Submit any Serverless job with `--native-mode AUDIT`. | The launcher rejects the mode before ordinary Zingg execution. |
| MT-058 | Invalid mode | Try lowercase, whitespace, an unknown mode, and a missing value. | Valid values are normalized where documented; invalid or missing values fail with an actionable message. |
| MT-059 | System-property activation | Use `-Dzingg.native.mode=STRICT` on Classic/Dedicated and verify the provider context. | The property activates the same registry as the launcher flag. |
| MT-060 | Environment activation | Use `ZINGG_NATIVE_MODE` for local tooling and omit the JVM property. | The documented local/Dedicated fallback is honored; Serverless does not depend on environment propagation. |
| MT-061 | Precedence | Set conflicting JVM property, environment value, launcher flag, and Python config. | The observed precedence is documented and deterministic; Serverless launcher arguments win for the managed job. |
| MT-062 | Unknown operation | Run `resources/serverless-failure-probes.yml` job `zingg-native-serverless-strict-unknown-operation`. | The job fails with an unknown/unmapped operation; it does not execute the legacy implementation under a native label. |
| MT-063 | Disabled operation ID | Disable a required operation by its operation ID. | `STRICT` fails closed and identifies the disabled operation. |
| MT-064 | Disabled rule ID | Disable a required rule such as `rewrite.similarity.exact`. | `STRICT` fails closed and identifies the rule. |
| MT-065 | Multiple disabled rules | Disable several rules, including one used and one unused by the fixture. | The used rule fails; the unused rule does not create a false failure. |
| MT-066 | Disabled rule in `REWRITE` | Disable a required rule in `REWRITE`. | The behavior is explicit and observable; it must not silently fall back while reporting native completion. |
| MT-067 | Unreachable Bigram function | Force `BigramJaccSimFn` through a custom/non-standard fixture. | `STRICT` rejects it as unsupported/unreachable; no approximate Jaccard alias is used. |
| MT-068 | Unsupported source-present function | Force `StringLengthFunction` or `SetMembershipFunction` if reachable in the supplied Zingg path. | The operation is classified and fails closed unless an exact registered rewrite exists. |
| MT-069 | Strict plan guard enabled | Run with `--native-plan-guard true` and inspect the analyzed plan. | Forbidden callback/object-encoder nodes are rejected in `STRICT`. |
| MT-070 | Strict plan guard disabled | Run a supported plan with `--native-plan-guard false`. | The configured guard behavior is honored, while rule observability and fail-closed operation policy remain intact. |
| MT-071 | Forbidden-node negative case | Create a controlled legacy/UDF/object-encoder plan in a test-only path. | `ScalaUDF`, `PythonUDF`, Python evaluation, `MapElements`, `MapPartitions`, serialization/object nodes, and `ExistingRDD` are rejected when the strict guard is enabled. |
| MT-072 | No silent fallback | Inspect logs and outputs for a deliberately disabled rewrite. | The result is failure or an explicitly legacy/audit result, never a successful “native” result backed by the original construct. |

## 4. Launcher and managed-session behavior

The production Serverless entry point is the Java wrapper
`ai.zingg.nativebridge.launch.DatabricksZinggMainEntry`, which invokes the
single Scala launcher implementation.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-080 | Managed runtime compatibility | Start a Serverless probe and inspect `SERVERLESS_RUNTIME_COMPATIBILITY`. | Spark is 4.x, Scala is 2.13, Java is 17, environment version is 5, transport is Spark Connect, and compatibility is `pass`. |
| MT-081 | DatabricksSession creation | Start a probe without an application-created session. | The launcher creates/reuses the managed `DatabricksSession` and ordinary Zingg can obtain it. |
| MT-082 | Active/default session binding | Run an ordinary phase and inspect session access from the patched Zingg code. | Both active and default session bindings are available before delegation. |
| MT-083 | Session cleanup | After success and after failure, inspect the task and follow-up query behavior. | Active/default application bindings are cleared; `SparkSession.stop()` is never called on the managed session. |
| MT-084 | Java static entry point | Use the resource main class exactly as configured. | Databricks resolves the Java entry class and it successfully invokes the Scala object. |
| MT-085 | Java/Scala delegate | Delegate to a Java static main and to a Scala object main in a controlled test. | Both invocation forms receive the forwarded arguments and failures propagate. |
| MT-086 | Missing delegate | Omit `--delegate-main` on an ordinary non-probe job. | The launcher fails with the documented missing-delegate error. |
| MT-087 | Delegate property | Set `-Dzingg.native.delegate.main` and omit the flag. | The property is accepted only as the documented fallback. |
| MT-088 | Argument forwarding | Add normal Zingg flags before and after native flags. | Native flags are consumed; all ordinary Zingg arguments are forwarded unchanged and in order. |
| MT-089 | Missing flag value | Omit a value after `--native-mode`, `--native-run-id`, `--native-disabled-rules`, `--zinggDir`, or a probe path. | The launcher fails early with the flag name in the error. |
| MT-090 | Boolean normalization | Test `true`, `false`, `1`, `0`, `yes`, `no`, `on`, and `off` for `--native-plan-guard`. | Accepted aliases normalize deterministically; other values fail. |
| MT-091 | Run ID propagation | Supply a known `--native-run-id`. | Diagnostics, materialization paths, cleanup, and phase summaries use the same correlation ID. |
| MT-092 | Run ID isolation | Launch two concurrent jobs with different IDs and the same `--zinggDir`. | Transient materialization paths do not collide and each job cleans only its own root. |
| MT-093 | Graph path containment | Supply a graph path below the run root and then one outside it. | The nested path is accepted; the outside path is rejected. |
| MT-094 | Missing `--zinggDir` | Run a Serverless model/graph/materialization path without `--zinggDir`. | The launcher refuses an unmanaged fallback path. |
| MT-095 | Interactive input injection | Use `--native-interactive-input` for controlled label/update-label probes. | The supplied input is consumed exactly once; normal jobs do not require a terminal. |
| MT-096 | Probe precedence | Pass multiple mutually exclusive probe flags. | The documented dispatch order is deterministic; ordinary delegate execution is not accidentally run. |
| MT-097 | Phase summary on success | Run an ordinary phase and inspect `NATIVE_PHASE_SUMMARY`. | One summary is emitted with phase, run ID, applied rules, fingerprints, and no row values. |
| MT-098 | Phase summary on failure | Run `resources/serverless-phase-failure-probe.yml`. | The task fails and still emits a failure-compatible summary/diagnostic. |
| MT-099 | Probe-specific evidence | Run a differential/probe job. | The probe emits its own authoritative marker and the launcher does not replace it with a misleading ordinary-phase summary. |

## 5. Transport parity and Python control surface

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-110 | Classic/Py4J startup | On a supported Classic/Dedicated Spark 4 session, load the core JAR and instantiate `ClassicBackend`. | Protocol version is `1`, library version starts with `0.3.0`, and Spark 4.x is accepted. |
| MT-111 | Classic missing core | Start `ClassicBackend` without the native core JAR. | `BackendUnavailableError` explains that the core JAR or patched Zingg artifact is missing. |
| MT-112 | Classic unsupported Spark | Point the Classic backend at a Spark 3.x session. | It fails instead of adding a compatibility shim. |
| MT-113 | Classic transform | Call the thin gateway with a binary operation and inspect the DataFrame. | The JVM registry computes the expression; Python does not implement the metric. |
| MT-114 | Classic unary misuse | Call `transform` without a right column. | The API rejects the call and directs the caller to `preprocess`. |
| MT-115 | Classic preprocessing | Call `preprocess` for trim/case normalization and compare with the JVM result. | The same public provider is used. |
| MT-116 | Connect backend | Create a managed Connect session and resolve `ConnectBackend`. | The backend is control/transport-only and uses public expressions through the common JVM path. |
| MT-117 | Python direct rewrite rejection | Call `PublicExpressionBackend.transform` or `.preprocess` directly. | It raises `NotImplementedError`; no Python semantic engine appears. |
| MT-118 | Runtime detection | Run `detect_runtime` on Classic, Connect, Databricks, and a non-Databricks Spark session. | API mode/platform/engine candidate are diagnosed, but `native_execution` remains observationally unset. |
| MT-119 | Photon configuration non-proof | Set `spark.databricks.photon.enabled=true` without query evidence. | Status reports a candidate only; it does not claim actual Photon execution. |
| MT-120 | `Zingg.status()` | Instantiate the Python control surface and inspect the returned dictionary. | It reports version, protocol, mode, backend, runtime, and capabilities without pretending to be a Zingg engine. |
| MT-121 | Transport parity output | Run the same fixture through Classic and managed Connect with the same run settings. | Row values, schema, candidates, model outputs, and phase outputs match; only transport metadata differs. |
| MT-122 | Transport parity failures | Trigger the same unknown/disabled operation over both transports. | Both fail with the same native policy semantics, not transport-specific fallback. |

## 6. Input readers, schema, row association, and ordinary data handling

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-130 | CSV input | Run the ordinary patched `SparkDFReader` path using a valid CSV fixture. | Input is read lazily, schema/values are preserved, and the phase completes. |
| MT-131 | JSON input | Repeat with JSON, including nested/nullable fields used by the config. | JSON is read through the ordinary reader and preserves expected nulls/types. |
| MT-132 | Parquet input | Repeat with Parquet and a Unity Catalog Volume path. | Parquet is read lazily and the native path does not inject an eager action. |
| MT-133 | Input-format matrix | Run `resources/serverless-input-format-probe.yml`. | CSV, JSON, and Parquet summaries all pass. |
| MT-134 | Empty input | Use an empty but valid input relation in each format. | The phase returns the upstream-defined empty result or a clear validation error; no hang or fabricated row is produced. |
| MT-135 | Missing optional input | Omit an optional training/label input. | Failure or empty behavior remains owned by upstream phase orchestration; the reader does not add an eager failure. |
| MT-136 | Missing required input | Point a required input at a nonexistent path. | The phase fails visibly with the input/path error. |
| MT-137 | Duplicate records | Include identical rows with different physical positions. | Row association and matching semantics do not collapse unrelated input rows. |
| MT-138 | Nulls and blanks | Include null, empty, whitespace-only, and mixed-null feature values. | Null/blank conventions match pinned Zingg behavior without exceptions or accidental stringification. |
| MT-139 | Unicode input | Include accents, non-Latin scripts, emoji, and supplementary-plane characters. | Values survive read/write and similarity/blocking semantics use the documented UTF-16 contract. |
| MT-140 | Dates and arrays | Include leap dates, null dates, empty arrays, null arrays, and arrays with null elements where the schema allows them. | Values, types, and native outputs match the oracle. |
| MT-141 | Column preservation | Add unrelated user columns, including `_native_similarity_row_id`. | User columns remain present and unchanged; the native materializer does not reserve or overwrite that name. |
| MT-142 | Schema order | Reorder input columns without changing configured feature names. | Output schema and feature selection follow the ordinary Zingg configuration, not incidental input order. |
| MT-143 | Type coercion | Try configured integer, long, float, double, string, date, boolean, and array fields with valid and invalid representations. | Valid coercions match upstream; invalid values fail clearly rather than silently changing type. |
| MT-144 | Row-ID probe | Run `resources/serverless-row-id-probe.yml`. | `_native_row_id` is non-null and unique within the action, preserves association, is not exposed as `z_zid`, and is not claimed stable across jobs. |
| MT-145 | Row-ID repeat | Run the same row-ID job twice. | Values may differ between actions/jobs; uniqueness and association invariants remain true. |
| MT-146 | Output schema contract | Compare ordinary Zingg output schema in OFF/AUDIT with REWRITE/STRICT. | Published Zingg columns and cluster IDs remain owned by upstream and are not replaced with native-only identifiers. |

## 7. Similarity differential tests

For every row in this section, run the native expression and the pinned
upstream class on the same fixture, then compare exact values, null behavior,
data types, and tolerances. Include equal, unequal, null, blank, case, Unicode,
surrogate-pair, punctuation, long, and malformed-but-schema-valid values.

| ID | Similarity family / edge set | Expected result |
|---|---|---|
| MT-160 | `SimilarityFunctionExact`: equal strings, unequal strings, null/null, null/value, empty/empty, empty/value | Exact upstream equality and null conventions. |
| MT-161 | `StringSimilarityFunction`: case, whitespace, Unicode normalization-sensitive values, punctuation | Exact upstream string behavior; no unrequested normalization. |
| MT-162 | `CheckNullFunction`: both null, left null, right null, neither null | Exact null indicator and numeric type. |
| MT-163 | `CheckBlankOrNullFunction`: null, empty, spaces, tabs, non-breaking space if supported, nonblank | Exact blank/null convention. |
| MT-164 | `IntegerSimilarityFunction`: equal, adjacent, negative, min/max, null | Exact numeric similarity, overflow behavior, type, and null result. |
| MT-165 | `LongSimilarityFunction`: equal, adjacent, negative, `Long.MinValue`, `Long.MaxValue`, null | Exact long boundary behavior. |
| MT-166 | `FloatSimilarityFunction`: equal, signed zero, negative, max/min, NaN, positive/negative infinity, null | Exact float semantics without accidental string conversion. |
| MT-167 | `DoubleSimilarityFunction`: equal, signed zero, negative, max/min, subnormal, NaN, positive/negative infinity, null | Exact double semantics and null behavior. |
| MT-168 | `DateSimilarityFunction`: same day, adjacent dates, leap day, epoch boundary, null | Exact date ratio and upstream date type conversion. |
| MT-169 | `ArrayDoubleSimilarityFunction`: equal arrays, different lengths, zeros, negative values, empty arrays, null arrays, null elements if valid | Exact cosine/array behavior, including zero-vector behavior. |
| MT-170 | `JaccSimFunction`: duplicate tokens, empty token sets, case, punctuation, repeated whitespace, null/blank | Exact tokenization and Jaccard behavior. |
| MT-171 | `NumbersJaccardFunction`: positive/negative numbers, duplicate numbers, decimal tokens, malformed tokens, empty input | Exact numeric token parsing and set semantics. |
| MT-172 | `ProductCodeFunction`: same code, prefix/suffix differences, separators, leading zeroes, null/empty | Exact product-code semantics. |
| MT-173 | `JaroWinklerFunction`: equal, empty, short strings, transpositions, common prefix, case, Unicode | Exact SecondString-compatible score within the documented tolerance. |
| MT-174 | `AJaroWinklerFunction`: the affine/Jaro variant's equal, short, transposed, null, and Unicode values | Exact upstream variant mapping; no alias to the wrong Jaro implementation. |
| MT-175 | `AffineGapSimilarityFunction`: equal, insertion, deletion, substitution, empty, null, short, 4/12/24-character inputs | Exact affine-gap/Monge-Elkan recurrence and expected nonlinear runtime are observed. |
| MT-176 | `EmailMatchTypeFunction`: exact address, case, local-part/domain differences, malformed address, null/empty | Exact email match classification. |
| MT-177 | `PinCodeMatchTypeFunction`: equal, one-digit difference, length differences, nonnumeric, null/empty | Exact PIN classification and numeric/string handling. |
| MT-178 | `OnlyAlphabetsExactSimilarity`: punctuation/digits removed or retained per upstream, empty after filtering, Unicode letters, null | Exact alphabet-only behavior. |
| MT-179 | `OnlyAlphabetsAffineGapSimilarity`: filtered equal/unequal values, short values, Unicode, empty/null | Exact filtered affine-gap behavior. |
| MT-180 | `SameFirstWordFunction`: one word, multiple words, repeated whitespace, punctuation, case, null/empty | Exact first-word extraction and comparison. |
| MT-181 | Similarity batch | Run several binary and unary feature rules in one batch. | Each output column is correct; columns do not cross-contaminate or change row order. |
| MT-182 | Fuzzy action isolation | Run `resources/serverless-fuzzy-action-probe.yml` and inspect Jaro and affine markers. | Each public expression can execute in an isolated relational action. |
| MT-183 | Fuzzy action scale | Run `resources/serverless-fuzzy-action-scale-probe.yml` with 100 rows. | Runtime, row count, and fingerprints are recorded; no timeout or silent downgrade occurs. |
| MT-184 | Fuzzy rule isolation | Run the Jaro-only and affine-only jobs in `resources/serverless-fuzzy-rule-probes.yml`. | Each selected rule is the only requested fuzzy rule and emits a successful comparison. |
| MT-185 | Full differential suite | Run `resources/serverless-zingg-native.yml` differential job with the full supported rule list. | All registered supported families match the pinned oracle for the fixture. |
| MT-186 | Similarity materialization parity | Compare materialized and non-materialized similarity feature frames. | Schema, columns, row count, values, nulls, and user columns are identical. |
| MT-187 | Materialization collision | Run `resources/serverless-similarity-materialization-probe.yml` with a user column named `_native_similarity_row_id`. | The user column is preserved; no synthetic key is created or dropped. |

## 8. Preprocessing tests

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-195 | Trim | Apply trim to leading, trailing, internal, blank, and null values. | Only the upstream-defined characters are removed; null remains null. |
| MT-196 | Case normalization | Apply case normalization to mixed case, locale-sensitive, Unicode, blank, and null values. | Output matches upstream; locale assumptions are not changed. |
| MT-197 | Stop-word differential | Run `resources/serverless-stopwords-differential-probe.yml`. | Native `regexp_replace` behavior matches the pinned `RemoveStopWordsUDF`, including the documented phrase fixture. |
| MT-198 | Stop-word pattern | Vary configured pattern, punctuation, word boundaries, repeated words, and case. | The supplied pattern is honored exactly and is observable as the selected rule. |
| MT-199 | Preprocessing column list | Apply preprocessing to one, many, duplicate, missing, and zero columns. | Valid columns are processed once; missing/empty lists fail with clear validation. |
| MT-200 | Preprocessing composition | Apply trim, case normalization, and stop words in the same ordinary phase. | Order is upstream-defined and feature generation sees the expected values. |
| MT-201 | Preprocessing parity across transports | Run the same preprocessing fixture through Classic and Connect. | Outputs and null handling match. |

## 9. Hash and blocking tests

Run the complete hash differential job and supplement it with boundary rows.
The expected hash is a candidate-generation input, not a published entity ID.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-210 | Hash differential suite | Run `resources/serverless-hash-differential-probe.yml`. | All registered hash families match the pinned reflection oracle. |
| MT-211 | First-character hashes | Test `first1Chars` through `first4Chars` with empty, short, exact-length, long, null, Unicode, and surrogate-pair values. | Java UTF-16 code-unit contract is preserved. |
| MT-212 | Last-character hashes | Test `last1Chars`, `last2Chars`, and `last3Chars` with the same boundaries. | Exact Java/SecondString behavior is preserved. |
| MT-213 | Last word | Test one word, many words, repeated whitespace, punctuation, empty, null, and Unicode. | Exact upstream last-word extraction. |
| MT-214 | Null/empty hash | Test null, empty, blank, and literal strings such as `null`. | Null/empty distinction matches upstream. |
| MT-215 | Identity hashes | Test string, integer, long, and boolean identities including null. | Value and output type are preserved. |
| MT-216 | Character boxes | Test `first2CharsBox` and `first3CharsBox` with short and long strings and null. | Boxed output and null conventions match upstream. |
| MT-217 | Decimal truncation | Test double/float truncation at positive, negative, exact, carry, NaN/infinity where valid, and null. | Truncation is not rounding and matches Java behavior. |
| MT-218 | Less-than-zero | Test double/float/int/long negative, zero, positive, min/max, NaN/infinity where valid, and null. | Exact Boolean/numeric hash output. |
| MT-219 | Trim-last-digits | Test double/float/int/long with fewer, equal, and more digits than the trim count, negatives, zero, boundaries, and null. | Java-compatible digit trimming and return type. |
| MT-220 | Numeric ranges | Test all 0–10, 10–100, 100–1000, and 1000–10000 families at lower/upper boundaries, negatives, large values, and null. | Boundary inclusivity and return type match upstream. |
| MT-221 | Java round | Test positive/negative halves, decimal precision, large values, signed zero, NaN/infinity where valid, and null. | Java-compatible rounding is preserved. |
| MT-222 | Blocking-tree simple node | Compile a one-node blocking tree and compare per-row hash values with upstream. | The public expression gives the same candidate hash. |
| MT-223 | Blocking-tree nested branches | Use nested AND/OR branches, multiple fields, null branches, and repeated nodes. | Compiled CASE/higher-order expression preserves tree semantics. |
| MT-224 | Candidate-set parity | Run `resources/serverless-blocking-differential-probe.yml`. | Candidate pairs match after duplicate suppression, not merely per-row hashes. |
| MT-225 | Blocking duplicate suppression | Include duplicate records and duplicate/reverse candidate pairs. | Candidate cardinality and deduplication match the ordinary Zingg contract. |
| MT-226 | Blocking unsupported hash | Force a tree containing an unregistered hash function. | `STRICT` fails closed with the hash/tree identifier. |
| MT-227 | Blocking tree rule disabled | Disable `rewrite.blocking.tree` or a referenced hash rule. | `STRICT` fails closed and reports the disabled rule. |
| MT-228 | Blocking expression limits | Construct trees at and above configured node/depth limits. | The safe tree succeeds; an oversized/unsafe tree fails explicitly. |
| MT-229 | Blocking across phases | Exercise the same tree through `findTrainingData`, `train`, `match`, and `link`. | The adapter does not produce phase-specific candidate semantics. |

## 10. Vector extraction and model-feature assembly

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-240 | Dense vector extraction | Run `resources/serverless-vector-probe.yml` with a dense `VectorUDT`. | Values and order match the upstream extractor. |
| MT-241 | Sparse vector extraction | Repeat with a sparse vector containing implicit zeros. | Expanded values have the expected length/order and no private Spark helper is required. |
| MT-242 | Null vector | Repeat with a null vector. | The native path matches upstream null behavior and does not silently fabricate zeros. |
| MT-243 | Empty vector | Test a zero-length vector if the schema permits it. | Empty behavior is explicit and stable. |
| MT-244 | Malformed vector struct | Supply a vector struct with missing or inconsistent fields in a controlled negative fixture. | The operation fails clearly rather than returning a shifted vector. |
| MT-245 | Vector rule disabled | Disable the vector extraction rule. | `STRICT` fails closed at the model boundary. |
| MT-246 | Feature order | Reorder feature definitions and compare model metadata and predictions. | Feature order follows upstream configuration and is persisted exactly. |
| MT-247 | Feature count | Test one, two, four, 11, and 20 configured features. | Correct polynomial shape/metadata is generated for each supported shape. |
| MT-248 | Feature null/finite policy | Include null, NaN, infinity, zero, negative, and large feature values. | The documented finite-value handling is applied consistently in fit and predict. |

## 11. Native model training, prediction, and persistence

The native model boundary is a replacement for the Spark-ML execution
representation, not a new Zingg training API. Compare against a Spark ML oracle
where the fixture and runtime support it.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-260 | Bounded model probe | Run `resources/serverless-model-semantic-probe.yml`. | The sidecar is created, metadata is internally consistent, and prediction columns are emitted. |
| MT-261 | Production grid metadata | Inspect a full sidecar. | `regParam` is from `{0.0001, 0.001, 0.01, 0.1, 1.0}`; threshold is from `{0.40, 0.45, 0.50, 0.55}`; max iterations/folds match the contract. |
| MT-262 | Polynomial term count | Test two features with degree 3 and compare term count/order to the Spark polynomial oracle. | Every term, including interaction/repetition terms, is in the Spark-compatible order. |
| MT-263 | Polynomial ordering fingerprint | Compare persisted `polynomialOrdering` with `spark-polynomial-expansion-order-v1`. | The sidecar has the documented ordering contract. |
| MT-264 | One-feature model | Fit/predict with one feature. | Degree and term metadata remain valid; no zero-term or index error occurs. |
| MT-265 | Four-feature model | Run `resources/serverless-four-feature-probe.yml` and inspect the persisted model. | Materialized/public term evaluation works for the multi-feature path. |
| MT-266 | Full 20-feature model | Run `resources/serverless-production-validation.yml` production task. | The 1,770-term path completes without quick-probe or bounded overrides and saves a model. |
| MT-267 | Independent soak | Run the performance-soak task with retries and inspect every attempt. | Repeated full-feature training is stable; retries do not hide deterministic correctness failures. |
| MT-268 | Row-level CV folds | Use duplicate feature/label rows and inspect fold assignments under a known seed. | Folds are assigned per row with public `rand(seed)`; duplicate rows can land in different folds. |
| MT-269 | CV fold repeat | Repeat the same run with the same seed and then a different seed. | Same seed is reproducible as documented; different seed can change fold assignment without content hashing. |
| MT-270 | Regularization selection | Create a fixture where candidate regularization values produce distinguishable metrics. | Selected value is one of the production grid values and tie behavior is documented. |
| MT-271 | Threshold selection | Create a fixture with distinguishable threshold metrics. | Selected threshold is one of the grid values and tie behavior is preserved. |
| MT-272 | Exact threshold prediction | Force a score exactly equal to the selected threshold. | Prediction is `0.0` because the contract is strict `score > threshold`. |
| MT-273 | Above threshold prediction | Force a score above the selected threshold. | Prediction is `1.0`. |
| MT-274 | Below threshold prediction | Force a score below the selected threshold. | Prediction is `0.0`. |
| MT-275 | Probability/raw prediction | Compare probability, raw prediction, prediction, and Zingg score columns. | Column names, types, values, and relationship match the Spark LogisticRegression contract. |
| MT-276 | Model save | Run `--native-model-probe --native-model-probe-path <Volume path>`. | `_zingg_native_model_v1` is written atomically/consistently with complete metadata and coefficients. |
| MT-277 | Same-job load | Save and load in one controlled job. | Loaded model reproduces metadata and predictions. |
| MT-278 | Cross-job load | Run `resources/serverless-model-cross-job.yml` train and load jobs separately. | A second process loads the sidecar and produces the same prediction fingerprint. |
| MT-279 | Cross-job path isolation | Use two model paths and two run IDs. | Each model is loaded from its explicit path; transient files do not cross-contaminate. |
| MT-280 | Model sidecar schema | Inspect Parquet schema and one row of metadata, without exposing sensitive input data. | Required fields, types, feature order, coefficients, intercept, grids, seed, and optimizer metadata are present. |
| MT-281 | Corrupt sidecar | Run `resources/serverless-model-failure-probe.yml`. | Load fails closed with the `model.nativePersistence.load` rule identifier. |
| MT-282 | Wrong schema version | Change the sidecar schema version in a disposable path. | Load rejects the unsupported version. |
| MT-283 | Missing feature metadata | Remove/rename feature metadata in a disposable sidecar. | Load rejects the incompatible sidecar. |
| MT-284 | Coefficient mismatch | Remove or add one coefficient. | Load rejects the coefficient/term-count mismatch. |
| MT-285 | Invalid grid value | Change `regParam` or threshold to a value outside the production grid. | Load rejects the value. |
| MT-286 | Legacy converter | On Dedicated/local Spark with ordinary Spark ML available, convert a real Zingg 0.7.0 `CrossValidatorModel`. | The converter writes a compatible native sidecar and leaves the legacy artifact untouched. |
| MT-287 | Legacy model prediction | Load the converted sidecar on a native path and compare predictions with the original legacy model. | Predictions and feature ordering match within the agreed numerical tolerance. |
| MT-288 | Legacy model in Serverless | Load only the converted sidecar on Serverless. | Serverless predicts without requiring the legacy Spark ML model or GraphFrames. |
| MT-289 | Model rule disablement | Disable fit, prediction, save, or load rule individually. | The specific operation fails closed and the other operations are not falsely credited. |
| MT-290 | Model quick probe isolation | Run quick/bounded diagnostic flags, then a production job. | Diagnostic overrides do not leak into the production task or release metadata. |

## 12. Ordinary Zingg phase tests

All phase tests must use the ordinary Zingg main/configuration. Do not replace a
phase with a Python or `zingg_native` API call.

| ID | Phase | Procedure | Expected result |
|---|---|---|---|
| MT-300 | `findTrainingData` smoke | Run the standard Serverless job from `resources/serverless-zingg-native.yml`. | The real phase completes in `STRICT`; blocking, similarities, row association, and output artifacts are valid. |
| MT-301 | `findTrainingData` candidate parity | Compare native candidates with pinned/upstream candidates on the same input. | Candidate pairs and duplicate suppression match. |
| MT-302 | `label` initial input | Run the tiny marked label probe with deterministic interactive input. | Labels are written to the ordinary Zingg output and the managed task returns normally. |
| MT-303 | `label` invalid input | Supply malformed, empty, out-of-range, and duplicate label responses. | Upstream validation/error behavior is preserved; no partial native state is misreported as success. |
| MT-304 | `updateLabel` input | Run the tiny marked update-label probe with a deterministic response. | Existing labels are updated according to upstream semantics and output schema. |
| MT-305 | `updateLabel` invalid input | Supply a response for a nonexistent pair or malformed pair. | The phase fails or rejects it according to upstream behavior. |
| MT-306 | `train` bounded | Run minimal/four-feature/tiny marked train jobs with bounded diagnostic options. | Native feature, vector, model fit, and sidecar boundaries execute successfully. |
| MT-307 | `train` production | Run full production validation with default model settings. | Full training completes without quick-probe/bounded overrides. |
| MT-308 | `match` existing model | Run tiny marked match after a successful train. | Existing model loads, predictions and clusters are produced, and published outputs are ordinary Zingg outputs. |
| MT-309 | `match` missing model | Point match at a missing or corrupt model directory. | The phase fails closed with an actionable model error. |
| MT-310 | `link` existing model | Run tiny marked link after a successful train. | Link/source semantics remain those of Zingg 0.7.0; graph consolidation uses the native boundary. |
| MT-311 | `link` missing model | Point link at a missing/corrupt model. | The phase fails visibly and does not create a false graph result. |
| MT-312 | Match/link distinction | Run equivalent `match` and `link` jobs and compare inputs, pair direction/source semantics, and output builders. | They are not aliases; each preserves upstream differences. |
| MT-313 | Phase repeatability | Repeat each bounded phase at least three times with the same release and fixture. | Results/fingerprints are stable where the contract promises determinism; any nondeterminism is documented and not semantic. |
| MT-314 | Phase cross-job handoff | Train in one job/process, then match/link in another. | Model/output paths and ordinary Zingg state are discoverable without relying on a live process. |
| MT-315 | Phase failure visibility | Use `resources/serverless-phase-failure-probe.yml` and inspect Databricks result state. | Databricks sees failure; launcher cleanup does not mask it or return success. |
| MT-316 | License handling | Run with a valid external license and then a missing/invalid license path. | Valid license works; invalid license fails; license content is not copied into logs or release artifacts. |
| MT-317 | Normal application compatibility | Run an existing ordinary Zingg 0.7.0 application with only the patched assembly/core and native mode enabled. | Application code and normal CLI/configuration remain unchanged. |

## 13. Native graph and connected-components tests

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-330 | Graph probe | Run `resources/serverless-graph-probe.yml`. | Nontrivial connected components converge and match the expected partition. |
| MT-331 | Isolated vertices | Include vertices with no edges. | Each isolated vertex remains its own component. |
| MT-332 | Self-edges | Include self-edges alongside ordinary edges. | Self-edges do not merge unrelated vertices or change component membership. |
| MT-333 | Duplicate edges | Include repeated identical edges. | Duplicates do not change membership or produce incorrect multiplicity. |
| MT-334 | Reverse edges | Include one-way and reverse copies. | Symmetrization and component membership match the upstream connected-components contract. |
| MT-335 | Chain graph | Test a chain requiring multiple iterations. | The algorithm converges to one component with the documented canonical label. |
| MT-336 | Star graph | Test a high-degree hub and leaves. | All connected leaves share the hub's component; no unrelated vertex joins. |
| MT-337 | Cycle graph | Test a cycle with a tail and isolated component. | Cycle/tail membership and disconnected partition are correct. |
| MT-338 | Multiple components | Test several components with non-contiguous IDs. | Components remain separate and canonical labels are correct. |
| MT-339 | ID types | Test IDs with strings, longs, negative values, and non-contiguous values where the schema permits. | Joins/group/min semantics preserve the configured ID type and ordering contract. |
| MT-340 | Duplicate vertex rows | Include duplicate vertex IDs. | Output cardinality and duplicate handling match the documented graph contract. |
| MT-341 | Convergence bound | Set `--native-graph-max-iterations` to a sufficient bound. | The graph succeeds when converged within the bound. |
| MT-342 | Low convergence bound | Set the bound below the required chain diameter. | Every mode fails closed with `Connected-components did not converge`; no partial result is published as valid. |
| MT-343 | Graph strategy | Run the supported `two_phase` strategy and an invalid strategy. | `two_phase` executes; unsupported strategy fails explicitly and is not confused with GraphFrames. |
| MT-344 | Graph materialization | Run with an explicit Volume materialization path below `--zinggDir`. | Iteration state is written under the scoped run root and is cleaned according to the lifecycle contract. |
| MT-345 | Graph path rejection | Use a path outside the run root, a parent path, and a symlink. | The launcher/lifecycle rejects unsafe paths before destructive operations. |
| MT-346 | Graph cleanup after success | Inspect transient graph generations after a successful probe. | Transient materialization is removed; published outputs remain. |
| MT-347 | Graph cleanup after failure | Force a graph failure and inspect the transient root. | The transient root is cleaned or the failure is explicitly reported; unrelated paths are untouched. |
| MT-348 | Graph benchmark | Run `resources/serverless-graph-benchmark.yml` with a fixed input and record query profiles. | Runtime, shuffle, iteration count, and component fingerprint are recorded without changing semantics. |
| MT-349 | LDBC datagen stress | Run `resources/serverless-datagen-stress.yml` on the prepared dataset. | The job completes within its budget and matches expected partition/report data. |
| MT-350 | LDBC `twitter_mpi` stress | Run the Twitter LDBC job with the expected ground truth. | Compare canonical `(vertex_id, component_id)` partitions, not only component count; record skew/runtime. |
| MT-351 | Graph rule disablement | Disable `rewrite.graph.connected_components`. | Native graph execution fails closed; GraphFrames fallback is not silently used in Serverless. |

## 14. Materialization lifecycle and cleanup safety

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-360 | UUID-scoped root | Run with a valid explicit run ID and inspect `NativeMaterializationLifecycle.runRoot`. | The transient root is below the configured Zingg directory and includes the run scope. |
| MT-361 | Invalid run ID | Supply an invalid UUID-like run ID in a materialization job. | The job rejects it rather than deriving an unsafe cleanup target. |
| MT-362 | Success cleanup | Run a successful materialized similarity/model/graph job. | Transient directories are removed after the launcher finally block. |
| MT-363 | Failure cleanup | Force a failure after creating transient data. | Cleanup still runs and does not hide the original failure. |
| MT-364 | Recovery pair | Run the failure and recovery jobs in `resources/serverless-materialization-recovery.yml`. | The expected failed sentinel is left, then the follow-up verifies that the transient root is absent. |
| MT-365 | Published-output preservation | Put a published model/output directory adjacent to the transient root and run cleanup. | Published model/output directories are not deleted. |
| MT-366 | Symlink file | Place a symlink file under a disposable root and run cleanup. | The symlink is rejected or treated safely; its target is never recursively deleted. |
| MT-367 | Symlink ancestor | Use a symlinked ancestor of a cleanup path. | Ancestor validation rejects the path. |
| MT-368 | Path traversal | Try `..`, alternate separators, root paths, and a path outside the Zingg directory. | The path is rejected before write/delete. |
| MT-369 | Repeated cleanup | Invoke cleanup twice or retry a failed task. | Cleanup is safe to retry and does not report a false destructive success. |
| MT-370 | Concurrent cleanup | Run two jobs with distinct roots concurrently and finish them in reverse order. | Each cleanup is isolated and neither job deletes the other's transient data. |
| MT-371 | User-column materialization | Repeat similarity materialization with arbitrary columns and nulls. | Full frame schema/values/row count survive write/read exactly. |
| MT-372 | Materialization path permissions | Use a read-only, nonexistent, and partially accessible Volume path. | The failure names the path/permission problem and does not leave an untracked partial release. |

## 15. Observability, evidence, and Photon verification

The adapter can record rule IDs, plan fingerprints, and output fingerprints, but
configuration is not evidence of Photon execution. Actual Photon claims require
Databricks runtime/query-profile evidence.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-380 | Rule observability | Run a fixture using one known rule and inspect diagnostics. | The exact rule ID is recorded once or in a documented deduplicated form. |
| MT-381 | Multiple-rule observability | Run a phase using similarity, hash, preprocessing, model, and graph rules. | Every applied boundary is represented; no row values or secrets are logged. |
| MT-382 | Disabled-rule evidence | Run a disabled-rule failure and inspect logs/evidence. | The operation/rule ID and run/phase are present. |
| MT-383 | Plan normalization | Run the same logical fixture twice and compare normalized plan fingerprints. | Runtime expression IDs/whitespace do not cause spurious fingerprint changes. |
| MT-384 | Output fingerprint | Run a deterministic fixture twice. | Output fingerprints are stable when ordering/contract requires stability; any unordered relation is canonicalized before comparison. |
| MT-385 | Plan guard diagnostics | Enable plan guard and inspect start/complete/rejection diagnostics. | Guard stages and rejection reason are visible without data leakage. |
| MT-386 | Serverless Photon participation | Run a successful Serverless production task, collect job-linked query-history metrics, and retain query IDs. | Nonzero `photon_total_time_ms` is recorded where present; result is labeled participation evidence, not operator-level certification. |
| MT-387 | Serverless zero-Photon query | Find a successful query with zero Photon time. | It is recorded as a limitation/fallback observation; the job is not declared fully Photon-native. |
| MT-388 | Dedicated Photon query profile | Run the Dedicated + Photon validation job, open Query Profile, and inspect actual operator/runtime metrics. | Photon execution is claimed only if the profile supports it; configuration alone is insufficient. |
| MT-389 | Dedicated fallback inspection | Inspect all relevant query stages for non-Photon fallback or unsupported nodes. | Any fallback is recorded and blocks a zero-fallback certification claim. |
| MT-390 | Evidence schema | Run `pwsh -File scripts/check-photon-evidence.ps1`. | Evidence has the required schema; `PASS` cannot contain fallbacks or missing run/plan identity. |
| MT-391 | Evidence identity | Cross-check workspace/job/run/task/query IDs in evidence against Databricks UI/CLI. | Evidence is job-linked and cannot be attributed to a different release. |
| MT-392 | Evidence release match | Compare evidence release ID and checksums with the deployed immutable release directory. | Results are not attributed to a later or different artifact. |
| MT-393 | Privacy review | Inspect driver logs, summaries, and evidence JSON with sensitive fixture values. | Rule diagnostics contain identifiers/fingerprints, not input row values, credentials, or license text. |

## 16. Performance, scale, and stability

These tests establish behavior under load; they do not turn a benchmark into
semantic or Photon certification.

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-400 | Local 100-row smoke | Run `python scripts/benchmark_native.py --rows 100 1000`. | Benchmark completes and records reproducible metadata. |
| MT-401 | Local 1K/100K profile | Run `python scripts/benchmark_native.py --rows 1000 100000`. | Graph, blocking, similarity, and aggregation results are correct and comparable across commits. |
| MT-402 | Dedicated 10M profile | Run the benchmark on Dedicated + Photon with 10M rows. | Record wall time, task time, shuffle, spill, Photon time, failures, and output fingerprint. |
| MT-403 | Serverless 10M profile | Run representative workloads on Serverless with the same fixture and compare resource/runtime data. | Record Serverless-specific startup, shuffle, timeout, and query-profile behavior. |
| MT-404 | Fuzzy length scaling | Run Jaro/affine actions at 4, 12, and 24-character inputs. | Runtime growth is recorded; exact recurrence remains in use and no approximate fallback is introduced. |
| MT-405 | Fuzzy row scaling | Run 4, 100, and production-shaped rows. | Plan size, query count, runtime, and failure behavior remain within the declared budget. |
| MT-406 | Full model scale | Run the 1,770-term production train with default convergence. | Completion and resource profile are recorded separately from bounded diagnostics. |
| MT-407 | Graph diameter scaling | Compare short chain, long chain, star, and dense graph workloads. | Iteration count, convergence, component fingerprint, shuffle, and spill are recorded. |
| MT-408 | Graph skew scaling | Use a high-degree hub and a dominant connected component. | No incorrect merge; skew and task imbalance are visible in the profile. |
| MT-409 | LDBC Graphalytics pressure | Run datagen and `twitter_mpi` jobs with ground truth. | Correct partition plus runtime/resource evidence is retained. |
| MT-410 | Retry stability | Configure retries on a production-like job and inspect all attempts. | Retries recover transient infrastructure errors only; they do not mask a repeatable semantic failure. |
| MT-411 | Long-running session stability | Run several ordinary phases sequentially in one managed task/job where supported. | No application-side session stop, stale active session, leaked transient path, or accumulating rule state appears. |
| MT-412 | Concurrent job stability | Run multiple jobs with different run IDs and separate output paths. | No cross-run data, rule state, model, or cleanup collision occurs. |
| MT-413 | Cache/persist prohibition | Inspect query plan/logs and source/bytecode checks for native model/graph paths. | Serverless native branches do not call `cache`, `persist`, `unpersist`, or `checkpoint`. |
| MT-414 | Query-plan growth | Compare feature batch versus one-operation materialization mode. | Materialization reduces Connect analysis pressure without changing schema/values/results. |

## 17. Security, deployment, and operational safety

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-420 | Secret scan | Search source, logs, bundle output, and release directory for credentials, tokens, and license contents. | No secret is committed, bundled, logged, or published. |
| MT-421 | External license | Confirm the license is supplied through the configured external path/secret and not embedded in JARs or fixtures. | License remains external and access-controlled. |
| MT-422 | Volume ACL | Run with a principal that can read inputs/write outputs but cannot read unrelated Volume paths. | Only configured paths are accessed; unauthorized paths fail safely. |
| MT-423 | Release immutability | Attempt to overwrite a published release directory or manifest. | The release process refuses mutation or creates a new release ID. |
| MT-424 | Partial upload | Interrupt or simulate failure during artifact publication. | Bundle/deployment verification rejects an incomplete release; no mixed-version set is used. |
| MT-425 | Remote checksum mismatch | Modify a disposable remote artifact and rerun verification. | Publisher/verification fails with a checksum mismatch. |
| MT-426 | Fixture publication | Run `pwsh -File scripts/publish-serverless-fixtures.ps1 ...` and inspect every referenced fixture. | Every non-secret fixture is generated/uploaded before job submission and paths resolve via bundle variables. |
| MT-427 | Job concurrency limit | Submit duplicate runs to a job with `max_concurrent_runs: 1`. | Databricks enforces the intended serialization; run IDs and outputs remain isolated. |
| MT-428 | Failure cleanup access | Deny cleanup permission after a controlled failure. | Original failure remains visible and cleanup warning is explicit; no broader deletion is attempted. |
| MT-429 | User data retention | Inspect transient roots, model sidecars, logs, and reports after successful/failing runs. | Only documented published artifacts remain; transient data is cleaned according to policy. |
| MT-430 | Malicious path inputs | Test absolute paths, URI-like paths, traversal, symlinked paths, and paths containing spaces. | Validation is fail-closed and limited to the intended workspace/Volume scope. |

## 18. Compatibility and final release sign-off

| ID | Manual test | Procedure | Expected result |
|---|---|---|---|
| MT-440 | Dedicated 17.3 end-to-end | Deploy the Dedicated 17.3 bundle with Photon, run all six phases, and collect profiles. | Ordinary Zingg works through the shared registry; Dedicated evidence is recorded independently. |
| MT-441 | Dedicated 18 LTS end-to-end | Repeat on the 18 LTS profile. | Same semantics and supported boundaries as 17.3, with documented runtime differences only. |
| MT-442 | Serverless environment 5 end-to-end | Deploy the Serverless release set and run the bounded phase suite plus production train. | Managed Connect execution, session lifecycle, model sidecar, graph path, and phases pass. |
| MT-443 | Classic/Py4J phase suite | Run the ordinary phase suite on Dedicated Classic/Py4J. | Transport changes do not change semantics or rule IDs. |
| MT-444 | Managed Connect phase suite | Run the same suite on Serverless/Connect. | Transport changes do not change semantics or rule IDs. |
| MT-445 | Existing model migration | Convert and consume an existing Zingg model on Dedicated, then load its sidecar on Serverless. | Migration is explicit, lossless within the documented contract, and legacy files remain intact. |
| MT-446 | Fresh-install validation | Deploy from a clean remote release directory with no prior native files. | All required JARs, patched assembly, fixtures, and configuration are supplied explicitly. |
| MT-447 | Upgrade validation | Deploy a new immutable release ID beside an old one and rerun the same fixture. | Old release remains recoverable; new release is the one identified by evidence. |
| MT-448 | Rollback validation | Point the bundle back to the previous known-good immutable release. | Rollback does not mutate the old artifact set or reuse transient state. |
| MT-449 | Full evidence review | Review all PASS/BLOCKED/FAIL records, logs, profiles, fingerprints, and known limitations. | No unverified semantic, Dedicated, or operator-level Photon claim is presented as certified. |
| MT-450 | Release gate decision | Compare results with `docs/release-readiness.md`, `docs/compatibility.md`, and the capability manifest. | Release status reflects actual evidence: Serverless runtime validation, Dedicated execution, semantic parity, Photon participation, and operator attribution are separately labeled. |

## Current known evidence boundary

At the time this catalog was added, the repository documentation records
substantial Serverless environment-5 runtime evidence, including ordinary
Zingg phases, bounded/full-shape native model paths, graph/materialization
probes, and query-history Photon participation. It does not automatically
close the following gates:

- exhaustive phase-level and model-oracle semantic parity;
- Dedicated + Photon execution in the configured workspace;
- operator-level Photon attribution for every rewrite;
- broad production-scale readiness for every input and graph shape;
- evidence that every successful query avoided fallback.

The manual result of a test must update the relevant evidence and readiness
document before changing any of those claims.
