# Zingg Native — Databricks Photon + Serverless adapter

`zingg-native` makes the **real Zingg 0.7.0 Spark workflow** suitable for native execution on Databricks Dedicated + Photon and Databricks Serverless without asking application code to use a second entity-resolution API.

The design is intentionally small:

```text
Zingg builds an operation
        -> native adapter recognizes the semantic operation
        -> UDF / Dataset.map / GraphFrames-specific implementation is replaced
           by an equivalent public Spark SQL/DataFrame expression
        -> Spark executes the rewritten plan
        -> STRICT mode/evidence detects anything that was not replaced
```

Upstream Zingg still owns phase orchestration, matching/linking semantics, labeling, candidate logic, feature definitions, and user-facing APIs. This project owns only the Databricks-native execution substitutions, including the Spark-ML training/prediction boundary and its native model sidecar when native mode is active.

## What is implemented

The Zingg 0.7 integration overlay redirects these Spark choke points to `NativeOperationProvider` in `REWRITE`/`STRICT` mode:

- similarity `callUDF` feature generation;
- hash-function `callUDF` and UDF registration;
- stop-word UDF preprocessing;
- blocking-tree `Dataset.map(new SparkBlockFunction(...))`;
- ML probability-vector UDF extraction;
- `SparkModel` assembler/polynomial/logistic/CrossValidator training and prediction, replaced with public DataFrame operations;
- native versioned model persistence plus a Dedicated/local legacy `CrossValidatorModel` converter;
- GraphFrames connected-components execution;
- Serverless-hostile DataFrame caching on native model/graph paths;
- `JavaSparkContext` lifecycle access in `ZinggSparkContext`.

The shared core contains public-expression rules for the registered Zingg hashes and the reachable similarity families, including the Jaro and affine-gap/Monge-Elkan paths. The blocking tree is compiled on the driver from Zingg domain objects into one Spark expression; Spark internals are not inspected or mutated.

## Build/integrate

Serverless production build (core + launcher):

```text
mvn -Pdatabricks-serverless-env5 package -DskipTests
```

For Dedicated Photon use `-Pdatabricks-dedicated-17.3` or `-Pdatabricks-dedicated-18-lts`. Build the optional legacy-model migration tool with `-Plegacy-model-converter`. The default reactor builds only the public-API `core` artifact. The abandoned custom Connect planner experiment is archived under `reference/legacy-connect-plugin/` and is **not** part of the production reactor or Serverless dependency set.

Apply the source hook to a Zingg 0.7.0 checkout:

```text
The overlay is stored under `integration/zingg-0.7.0-overlay`; this repository does not modify a Zingg checkout.
```

Deploy the native-core JAR together with a separately supplied Zingg 0.7.0 Spark 4 assembly. Existing Zingg application code remains unchanged.

## Activation

Modes are `OFF`, `AUDIT`, `REWRITE`, and `STRICT`; the adapter defaults to `STRICT`. Dedicated/local JVM execution can use `-Dzingg.native.mode=STRICT` (the bundle sets it automatically) and local tooling may use `ZINGG_NATIVE_MODE` as a fallback.

Serverless uses launcher arguments rather than environment-variable propagation:

```text
--delegate-main <real-zingg-main> --native-mode STRICT --native-plan-guard true
```

Optional launcher flags are `--native-run-id` and `--native-disabled-rules`.

## Validation status

Databricks-only validation covers real Zingg 0.7.0 `findTrainingData`, bounded
synthetic `train`, `match`, and `link` on Serverless environment 5 in `STRICT`
mode, including three deterministic match/link repeats and a balanced 100-row
match. Job-linked Databricks query-history metrics provide Photon runtime
evidence. Semantic differential parity, general large-data scale readiness, and Dedicated
Photon execution remain release gates.

See `docs/architecture.md`, `docs/databricks.md`, `docs/model-native.md`, and `integration/README.md`.

## Documentation and manual validation

The documentation is organized around the transparent adapter boundary:

- `docs/architecture.md` — execution model and ownership boundaries;
- `docs/internal-how-it-works.md` — detailed internal control flow, module
  responsibilities, rewrite lifecycle, model/graph paths, and evidence;
- `docs/rewrite-rules.md` and `docs/semantic-contracts.md` — registered rules
  and invariants;
- `docs/compatibility.md`, `docs/databricks.md`, and `docs/serverless.md` —
  supported Databricks targets and transport/runtime contracts;
- `docs/evidence/` — retained runtime and Photon evidence;
- `docs/manual-test-catalog.md` — exhaustive manual test checklist, expected
  results, evidence requirements, and release sign-off matrix.

The manual catalog separates Serverless runtime validation, Dedicated + Photon
validation, semantic parity, and operator-level Photon attribution. A
successful job or a Photon configuration flag is not, by itself, proof of any
of the latter claims.
