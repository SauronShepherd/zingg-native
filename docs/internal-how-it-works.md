# How `zingg-native` works internally

This document explains the implementation as it exists in this repository. It
is an engineering description of the execution path, not a user-facing API
guide and not a claim that every path is already semantically or operationally
certified.

## The short version

`zingg-native` is a transparent execution adapter around the real Zingg 0.7.0
Spark implementation. Zingg still creates the job, owns phase orchestration,
defines features and candidates, manages labels, and builds the final match and
link outputs. The adapter only intercepts selected Spark-specific construction
boundaries and replaces them with equivalent public Spark expressions or
relational DataFrame operations.

The central flow is:

```text
ordinary Zingg 0.7.0 application
        |
        v
patched Zingg Spark boundary in integration/zingg-0.7.0-overlay
        |
        v
NativeOperationProvider
        |
        +--> semantic operation lookup
        +--> rule enabled/disabled check
        +--> public Column / DataFrame / Dataset operation
        +--> plan guard and rule evidence
        |
        v
Spark 4.x planner and Databricks runtime
        |
        +--> Classic/Py4J on Dedicated
        +--> managed Spark Connect on Serverless
```

The adapter changes the execution representation, not the Zingg semantics.
There is no parallel entity-resolution engine and users do not rewrite normal
Zingg jobs to a second API.

## Design constraints that shape the implementation

The implementation follows a few non-negotiable boundaries:

1. The pinned `reference/upstream-zingg` checkout is the Zingg 0.7.0 semantic
   reference. It is immutable and is not part of the production edit path.
2. Production integration is stored in
   `integration/zingg-0.7.0-overlay` and calls the native core at known Spark
   choke points.
3. The supported runtime family is Spark 4.x, Scala 2.13, and Java 17.
4. The mandatory Databricks targets are Dedicated + Photon and Serverless.
5. Common and Serverless rewrites use public Spark APIs: `Column`,
   `Dataset<Row>`, DataFrame, Spark SQL functions, and public Spark ML data
   types where needed.
6. The production path does not use Python UDFs, Scala UDFs, Catalyst APIs,
   planner extensions, `SparkSessionExtension`, `SparkContext`,
   `JavaSparkContext`, or a custom Spark Connect server plugin.
7. Classic/Py4J and managed Spark Connect are transport variants over one
   rewrite registry. They are not two semantic implementations.
8. `STRICT` fails closed. An unknown, disabled, or unsafe operation is an
   explicit failure, not a silent legacy fallback advertised as native.
9. Photon execution is established by Databricks runtime/query-profile
   evidence, never by a configuration flag alone.
10. Each rewrite is observable and can be disabled individually.

## Repository layers

| Layer | Location | Responsibility |
|---|---|---|
| Semantic reference | `reference/upstream-zingg` | Pinned Zingg 0.7.0 behavior and edge-case oracle. Never edit. |
| Integration overlay | `integration/zingg-0.7.0-overlay` | Small source changes at the upstream Spark boundaries. It preserves Zingg phase code and ordinary APIs. |
| Shared native core | `core/src/main/scala/ai/zingg/native` | Modes, operation identity, registry, public expressions, model engine, graph algorithm, lifecycle, diagnostics, and evidence. |
| Classic gateway | `core/src/main/scala/ai/zingg/native/gateway/ClassicGateway.scala` | Optional Py4J transport surface for Dedicated/local use. It exposes the same registry and no phase logic. |
| Serverless launcher | `serverless-launcher/src/main` | Managed `DatabricksSession` bootstrap, argument filtering, probe dispatch, delegation, and managed-session cleanup. |
| Python surface | `python/src/zingg_native` | Configuration, runtime detection, control/status, and transport selection. It does not implement similarity or entity-resolution semantics. |
| Deployment resources | `resources`, `databricks.yml`, `scripts` | Asset Bundle jobs, artifact publication, fixture publication, static gates, and runtime probes. |
| Documentation/evidence | `docs` | Contracts, runbooks, evidence records, readiness state, and manual acceptance tests. |

## The shared execution context

Every provider call is associated with a `RewriteContext`. It carries:

- the current `SparkSession`;
- the parsed `NativeExecutionMode`;
- runtime metadata such as Spark and Scala versions;
- the Zingg phase or boundary name;
- a correlation/run ID;
- disabled operation/rule identifiers;
- operation-specific parameters such as stop-word patterns.

`NativeOperationProvider.fromSpark` builds this context from JVM properties and
the documented local environment fallback. The main properties are:

```text
zingg.native.mode
zingg.native.run.id
zingg.native.disabled.rules
zingg.native.plan.guard
zingg.native.transport
zingg.native.platform
```

Serverless launcher flags are converted to JVM properties before the real
Zingg main starts. This is important because a managed Serverless task cannot
rely on arbitrary environment-variable propagation in the same way as a local
or Classic process.

## Execution modes

The mode is represented by `NativeExecutionMode` in
`RewriteArchitecture.scala`.

| Mode | Rewrite known operations | Audit legacy boundaries | Fail closed for unknown native operations | Serverless |
|---|---:|---:|---:|---:|
| `OFF` | No | No | No | Rejected by launcher |
| `AUDIT` | No | Yes | No | Rejected by launcher |
| `REWRITE` | Yes | Yes | Only where the invoked boundary explicitly rejects | Supported |
| `STRICT` | Yes | Yes | Yes | Supported and default |

`OFF` and `AUDIT` preserve the legacy Dedicated/local compatibility path. For
example, the overlay can continue to use the ordinary Spark UDF, GraphFrames,
or Spark ML implementation when those dependencies are available. Serverless
does not accept those modes because its production contract is the public-API
native path.

`REWRITE` resolves and applies registered rules but is less strict about plan
compatibility. `STRICT` applies the same rules and additionally rejects
unknown/disabled operations and known forbidden plan constructs. The mode is a
policy boundary, not a second algorithm.

## Operation identity and registry resolution

The adapter resolves semantic operations, not generated UDF names. This avoids
collisions caused by registration names and keeps mapping tied to the concrete
Zingg function class or configured hash-function name.

`NativeOperation` inventories the operation families in
`RewriteArchitecture.scala`:

- registered similarity function classes;
- blocking/hash function names;
- preprocessing operations (`trim`, `caseNormalize`, `stopWords`);
- model operations (`vectorValue`, native logistic/CV, prediction, save, load);
- graph connected components;
- blocking-tree compilation.

`RewriteRegistry` maps an operation ID to a `RewriteRule`. A rule contains:

- a stable rule ID;
- the semantic `NativeOperation` it implements;
- a function from public Spark `Column` values to a public Spark `Column`;
- access to the current `RewriteContext`.

The normal resolution path is:

```text
upstream semantic class/name
        -> NativeOperationProvider mapping
        -> operation ID
        -> RewriteRegistry.resolve
        -> disabled check
        -> NativeEvidenceCollector.recordRule
        -> RewriteRule(Column, Column?, context)
```

An unknown operation is not inserted into the registry dynamically. Registry
resolution fails with `NativeRewriteUnsupportedException`, which is the
intended fail-closed behavior in `STRICT`.

## What the overlay intercepts

The overlay does not wrap the whole Zingg phase. It changes the implementation
at reusable Spark boundaries reached by multiple phases.

| Upstream boundary | Legacy representation | Native representation |
|---|---|---|
| `SparkBaseTransformer` / `SparkTransformer` | Similarity `callUDF` and UDF registration | Registered public `Column` expressions selected by semantic class |
| `SparkHashFunction` / `SparkHashUtil` | Hash `callUDF` and UDF registration | Public hash `Column` expressions and registration bypass |
| `SparkStopWordsRemover` | Stop-word Scala UDF | Public `regexp_replace` expression |
| `SparkBlockingTreeUtil` | Typed `Dataset.map` with `MapFunction`/encoder | Driver-compiled blocking-tree IR represented as a public expression |
| `VectorValueExtractor` | Vector extraction UDF | Public extraction from the VectorUDT values representation |
| `SparkModel` | `VectorAssembler`, `PolynomialExpansion`, `LogisticRegression`, `CrossValidator` | `NativeModelEngine` using public projections, aggregates, windows, and a native sidecar |
| `SparkGraphUtil` | GraphFrames connected components | Public relational two-phase label propagation |
| `ZinggSparkContext` | Application access to `JavaSparkContext` lifecycle | Managed `SparkSession` only in native Serverless flow |
| `SparkDFReader` | Input loading boundary | Public `DataFrameReader.load` while preserving laziness |
| `DFUtil` row association | RDD `zipWithUniqueId` | `monotonically_increasing_id()` as an opaque within-action association key |
| Managed invocation | Upstream CLI `System.exit` cleanup behavior | Invocation guard returns control to the Databricks task |

The original implementations remain available only where the selected mode and
artifact support them. In native Serverless execution, the replacement is
selected before the unsupported construct is emitted.

## Row-level public-expression rewrites

`PublicRewriteRules.scala` contains the expression implementations. They are
built from public Spark SQL/DataFrame functions such as `when`, `cast`,
`regexp_extract_all`, `array_distinct`, `array_intersect`, `transform`,
`aggregate`, `zip_with`, `sequence`, `conv`, `hex`, `encode`, and ordinary
arithmetic/comparison expressions.

The important consequence is that Spark receives a relational expression tree,
not a JVM callback to execute for every row. The same logical expression can be
serialized by Spark Connect or planned by Classic Spark.

### Java and SecondString compatibility

Several upstream algorithms are defined in terms of Java strings and
SecondString behavior. Spark SQL string operations do not always use the same
unit model, particularly for supplementary Unicode characters. The native
implementation therefore represents Java UTF-16 code units through public
encoding/hex expressions when needed.

This is used for behaviors such as:

- first/last character blocking hashes;
- Java-compatible string hashing;
- Jaro matching windows and character comparison;
- affine-gap/Monge-Elkan operations;
- Java-compatible numeric wrapping and rounding.

Null, empty, blank, tokenization, date, array, and numeric edge behavior is
part of the semantic contract. A merely similar metric is not an acceptable
replacement in `STRICT`.

### Similarity batching

At the normal similarity boundary, the provider maps all requested concrete
Zingg similarity classes to a batch of output columns. Each rule is resolved
and recorded independently, even when the expressions are applied in one
projection.

Managed Spark Connect can receive a very large plan for several dynamic fuzzy
expressions. When the launcher supplies a similarity materialization root, the
provider deliberately creates one public relational boundary per operation:

```text
input frame
  -> compute one similarity Column
  -> write complete frame to a temporary Parquet path
  -> read complete frame
  -> compute the next similarity Column
```

This changes plan construction and action boundaries, not row association or
feature semantics. The complete frame is written and reread; the path does not
depend on a synthetic `_native_similarity_row_id` column.

## Blocking-tree compilation

Blocking trees are ordinary Zingg domain objects. They are compiled on the
driver into `NativeBlockingTreeIR`, a small internal representation containing
the tree's operators, fields, hash functions, and branches.

The compiler then builds one public `Column` expression that computes the same
per-record blocking hash. It uses a Java-compatible hash expression over the
compiled suffix and preserves the candidate-set semantics of the upstream
tree.

The compiler has safety limits for node count and depth. A disabled hash, an
unknown tree shape, or a tree that would create an unsafe expression plan
raises `NativeRewriteUnsupportedException`. The compiler does not inspect or
mutate Catalyst plans.

## Native model path

The model path is a replacement for the Spark execution boundary inside
`SparkModel`; it is not a replacement for Zingg's training phase.

### Training flow

```text
Zingg creates positive/negative training rows
        -> upstream similarity feature columns
        -> native feature frame/materialization when required
        -> public degree-3 polynomial term expressions
        -> public relational standardization/statistics
        -> public SQL aggregate-based logistic optimizer
        -> row-level cross-validation selection
        -> final coefficients/intercept/threshold
        -> _zingg_native_model_v1 sidecar
```

The current native model contract stores and uses:

- polynomial degree 3;
- Spark-compatible polynomial term ordering;
- regularization grid `0.0001`, `0.001`, `0.01`, `0.1`, `1.0`;
- threshold grid `0.40`, `0.45`, `0.50`, `0.55`;
- two-fold selection;
- seed `13`;
- default maximum of 100 optimizer iterations;
- versioned optimizer identifier `lbfgs-public-sql-v1`.

The polynomial paths are represented as data and evaluated using public higher-
order SQL expressions for large managed Connect plans. Terms and margins are
also processed in bounded batches so the production 1,770-term shape does not
become one oversized remote expression.

The optimizer uses public Spark actions and relational aggregates for feature
statistics, gradients, loss, and candidate selection. It does not call
`LogisticRegression.fit`, `CrossValidator.fit`, RDD transformations, UDFs, or
`SparkContext` in the native path.

Cross-validation folds are assigned per row with `rand(seed)`. They are not
content-hashed, so duplicate feature/label rows remain independent training
rows.

### Prediction flow

```text
input feature columns
        -> native polynomial expansion in persisted order
        -> linear margin
        -> logistic probability
        -> raw prediction / score / prediction columns
```

Prediction uses the strict Spark LogisticRegression boundary:

```text
prediction = 1.0 when score > threshold
prediction = 0.0 otherwise
```

An exact threshold score is therefore negative (`0.0`).

### Persistence flow

Native models are saved below the ordinary Zingg model directory in the
versioned `_zingg_native_model_v1` Parquet sidecar. The sidecar records feature
order, schema version, polynomial contract, coefficients, intercept,
regularization, threshold, folds, seed, iteration count, and optimizer
contract.

Loading validates the version, feature count, coefficient count, polynomial
contract, and allowed grid values. A corrupt or incompatible sidecar fails
closed with the persistence rule identifier.

The optional `legacy-model-converter` runs only where ordinary Spark ML model
loading is available, normally Dedicated/local Spark. It extracts the best
legacy fitted pipeline and writes the native sidecar. It does not modify or
delete the original `CrossValidatorModel` files. Serverless can then load the
sidecar without loading the legacy Spark ML model.

## Native graph path

`NativeGraph.connectedComponents` is a public-relational port of GraphFrames'
default `two_phase` algorithm.

The flow is:

1. Select distinct vertex IDs.
2. Normalize edges into source/destination columns.
3. Add reverse edges, remove null/self edges, canonicalize endpoint order, and
   remove duplicate edges.
4. Materialize the initial edge relation under a run-scoped temporary path.
5. Compute minimum-neighbor labels with public `groupBy`, `min`, and `count`.
6. Apply large-star and small-star transformations with joins, projections,
   filters, unions, and distinct operations.
7. Materialize and reread each iteration.
8. Detect convergence using the aggregate minimum-neighbor sum.
9. Join component labels back to the original vertices.

The algorithm requires a positive maximum-iteration bound and only accepts the
`two_phase` strategy. If convergence is not reached, it fails closed in every
mode rather than returning a partial graph result.

Native Serverless graph execution has no GraphFrames compile-time or runtime
dependency. GraphFrames is retained only for the legacy OFF/AUDIT compatibility
branch where the surrounding Dedicated artifact supplies it.

## Serverless launcher lifecycle

The Serverless Asset Bundle uses the Java entry class
`ai.zingg.nativebridge.launch.DatabricksZinggMainEntry`. The Java wrapper
exists so Databricks can resolve a conventional static `main`; it reflectively
invokes the Scala object
`ai.zingg.native.launch.DatabricksZinggMain`.

The launcher lifecycle is:

```text
Databricks invokes Java static main
        -> parse and consume native launcher flags
        -> preserve/forward ordinary Zingg arguments
        -> set JVM properties for mode/run/rules/probes
        -> create managed DatabricksSession
        -> validate Spark 4.x / Scala 2.13 / Java 17 / environment 5
        -> set active/default SparkSession
        -> dispatch a probe or invoke the real Zingg main
        -> emit phase/probe evidence
        -> clear active/default session bindings
        -> clean the run-scoped transient materialization root
```

The launcher never calls `SparkSession.stop()` because Databricks owns the
managed session lifecycle. The upstream Zingg CLI's process-exit behavior is
guarded in the managed overlay so task success/failure remains visible to
Databricks.

Native-only diagnostic flags are consumed by the launcher and are not passed
to the ordinary Zingg main. Normal Zingg arguments such as `--phase`, `--conf`,
`--zinggDir`, and `--license` are forwarded unchanged.

## Temporary materialization and cleanup

Serverless native model, similarity, margin, and graph paths require an
ordinary `--zinggDir`. The launcher derives a root similar to:

```text
<zinggDir>/.native-transient/<UUID>/
```

Subpaths are used for the relevant boundary, for example:

```text
base/
base/similarity/
base/margins/
graph/
```

Cleanup is owned by `NativeMaterializationLifecycle` and is deliberately
restrictive:

- the run ID must be a valid UUID;
- the target must match the run-scoped `.native-transient/<UUID>` shape;
- the target and every ancestor are checked for symbolic links;
- real paths must remain below the canonical run root;
- repeated cleanup is harmless;
- published model/output directories are outside the transient deletion scope.

If a configured graph materialization path is supplied, the launcher requires
it to be below the derived run root. It refuses an unmanaged fallback path when
the required `--zinggDir` is absent.

## Plan guard and fail-closed behavior

`NativePlanGuard` operates at a known Zingg boundary after the public
expression has been built. In `STRICT`, it can inspect the physical plan and
reject known non-native constructs, including:

```text
ScalaUDF
PythonUDF
BatchEvalPython
ArrowEvalPython
MapElements
MapPartitions
SerializeFromObject
DeserializeToObject
ExistingRDD
```

The similarity batch boundary is treated specially because fully expanding
many Jaro/affine expressions into a remote plan is itself disproportionately
expensive; every operation is still resolved and checked through the registry
before that boundary is constructed.

The guard does not rewrite Catalyst. It only inspects a plan string at the
integration boundary and rejects known forbidden constructs when strict policy
requires it.

Fail-closed cases include:

- unknown operation or semantic class;
- explicitly disabled operation or rule;
- unsupported source-present/non-standard function such as `BigramJaccSimFn`;
- unsupported blocking-tree hash or unsafe tree size/depth;
- invalid/corrupt native model sidecar;
- non-converged graph;
- invalid Serverless mode or launcher argument;
- missing/unsafe materialization root;
- unsupported runtime family.

The legacy implementation is used only when the selected compatibility mode
and artifact explicitly permit it. A failure in a native-only Serverless branch
is not silently converted into a legacy GraphFrames or Spark ML run.

## Evidence and observability

The system has two related but separate evidence layers.

### Rule and phase diagnostics

`NativeDiagnostics` logs privacy-safe records containing identifiers and
metadata, not input row values. It can record:

- applied rewrite rule and operation boundary;
- phase and correlation/run ID;
- native and upstream Zingg versions;
- Spark runtime version;
- model stages;
- graph iteration/convergence;
- plan-guard stages and failures;
- phase summaries.

The `once` map prevents duplicate log noise for the same run, phase, event, and
rule while `NativeEvidenceCollector` retains applied rule IDs for the run.

### Plan/output evidence

At an explicit evidence seam, `NativeEvidenceCollector` can capture:

- normalized plan fingerprint;
- applied rule IDs;
- runtime descriptor;
- a row-count plus sorted row-digest multiset fingerprint;
- optional externally supplied Photon evidence;
- correlation/run ID.

Raw row values are not emitted by the output fingerprint. A phase that does not
expose a final DataFrame can still emit a summary with the plan fingerprint
explicitly marked unavailable rather than fabricating evidence.

### Photon evidence

The runtime descriptor can identify a Databricks/Photon candidate, but it does
not prove that a query actually executed in Photon. Actual Photon claims must
be tied to Databricks job/query history or Query Profile evidence, including
the deployed release identity. Nonzero Photon time proves participation for the
linked queries; it does not automatically prove that every operator or every
rewrite avoided fallback.

## Transport architecture

The semantic implementation is shared:

```text
Classic/Py4J                         managed Spark Connect
      |                                      |
      v                                      v
ClassicGateway / overlay hooks       DatabricksSession / launcher
      |                                      |
      +------------ same NativeOperationProvider ------------+
                                   |
                                   v
                         same RewriteRegistry and rules
```

The Classic gateway exposes library/protocol versions, operation metadata, and
small transform/preprocess methods for control clients. The Python
`ClassicBackend` can set JVM properties and invoke it through the supported
Py4J bridge.

The Python `ConnectBackend` is intentionally transport/control-only. It does
not reproduce similarity, hash, model, or graph logic in Python. In production
Serverless, the JVM launcher and patched Zingg code own those semantics.

The historical custom Connect expression/planner plugin under
`reference/legacy-connect-plugin` is archived and must not appear in the
production reactor or dependency set.

## What remains upstream-owned

These behaviors are deliberately not reimplemented by `zingg-native`:

- phase orchestration and CLI behavior;
- user-facing Zingg configuration and normal application APIs;
- positive/negative training-row construction;
- label acquisition and label-update semantics;
- blocking-tree domain semantics and artifact ownership;
- match versus link input/source differences;
- final output construction and published cluster identifiers;
- Zingg model directory conventions outside the native sidecar;
- the semantic source of truth for all edge cases.

This ownership split is why a normal Zingg job can be run with the adapter
without being rewritten as a different application.

## Typical end-to-end examples

### Dedicated + Photon

```text
1. Databricks starts the ordinary patched Zingg main on a Photon cluster.
2. Spark creates a Classic/Py4J-compatible SparkSession.
3. The overlay calls NativeOperationProvider at similarity, blocking, model,
   graph, preprocessing, and lifecycle boundaries.
4. The provider resolves public-expression rules under STRICT policy.
5. Spark plans and executes the resulting relational operations.
6. Rule, plan, output, and query-profile evidence is collected separately.
```

### Serverless

```text
1. Databricks resolves DatabricksZinggMainEntry.
2. The wrapper invokes DatabricksZinggMain.
3. The launcher consumes native flags and forwards normal Zingg arguments.
4. A managed DatabricksSession is created and validated.
5. The real patched Zingg main runs unchanged from the application's point of
   view.
6. Native public expressions are transported through managed Spark Connect.
7. Model/graph/similarity transient paths are scoped below --zinggDir.
8. The task emits evidence, clears session bindings, and cleans its run root.
```

## Where to inspect the implementation

| Question | Start here |
|---|---|
| How are modes, operation IDs, registries, guard, and evidence defined? | `core/src/main/scala/ai/zingg/native/RewriteArchitecture.scala` |
| How does a public operation become a DataFrame column? | `core/src/main/scala/ai/zingg/native/Core.scala` and `PublicRewriteRules.scala` |
| How does patched Zingg call the core? | `core/src/main/scala/ai/zingg/native/NativeOperationProvider.scala` and `integration/zingg-0.7.0-overlay` |
| How are similarity classes mapped? | `NativeOperationProvider.similarityByZinggName` and `similarityBatchByZinggName` |
| How is the blocking tree compiled? | `NativeBlockingTreeIR.scala` and `BlockingTreeCompiler.scala` |
| How is the model trained/persisted/predicted? | `NativeModel.scala` and `integration/.../SparkModel.java` |
| How are components computed? | `NativeGraph.scala` and `integration/.../SparkGraphUtil.java` |
| How are transient paths cleaned? | `NativeMaterializationLifecycle.scala` and `DatabricksZinggMain.scala` |
| How does Serverless start and delegate? | `DatabricksZinggMainEntry.java` and `DatabricksZinggMain.scala` |
| How does Classic/Py4J reach the core? | `ClassicGateway.scala` and `python/src/zingg_native/backend/classic.py` |
| What is currently evidenced? | `docs/evidence/`, `docs/testing.md`, and `docs/release-readiness.md` |
| What should be manually exercised? | `docs/manual-test-catalog.md` |

## Current validation boundary

The implementation and evidence documents currently distinguish runtime
validation from certification. Serverless environment-5 runs provide runtime
evidence for substantial ordinary Zingg and native paths. Dedicated + Photon
execution, exhaustive semantic/model-oracle parity, broad scale readiness, and
operator-level Photon attribution remain separate release gates until their
own evidence is collected and linked to the exact deployed artifact.
