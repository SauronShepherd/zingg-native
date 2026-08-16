# Upstream-to-native phase map

**Phase parity is not yet implemented in the shared Scala core.** The entries
below are a contract inventory, not release claims. The former Python phase
shortcuts remain available only through the explicitly named prototype backend.

The upstream Zingg 0.7 phase executors are the baseline, not the native
implementation. The native adapter must preserve their phase contracts while
replacing their Spark-sensitive internals.

| Phase | Upstream executor | Native boundary | Status |
|---|---|---|---|
| findTrainingData | `SparkTrainingDataFinder` | candidate-pair relation and persistence | contract inventory; shared-core implementation open |
| label | `SparkLabeller` | read/write pair relation; interactive UI excluded from non-interactive run | contract inventory; shared-core implementation open |
| updateLabel | `SparkLabelUpdater` | deterministic label merge/update | contract inventory; shared-core implementation open |
| train | `SparkTrainer` | feature construction and model persistence | contract inventory; shared-core implementation open |
| match | `SparkMatcher` | scoring, thresholding, clustering, output | contract inventory; shared-core implementation open |
| link | `SparkLinker` | cross-source candidate generation and scoring | contract inventory; shared-core implementation open |
| generateDocs | `SparkDocumenter` | metadata/document output | contract inventory; shared-core implementation open |

The upstream checkout at `../zingg` is read-only. Each phase will be run there
as a semantic and plan oracle, then implemented behind `zingg_native` with
ordinary Spark 4 DataFrame/SQL expressions where possible.

## Initial incompatibility inventory

The upstream Spark implementation contains several native-engine blockers:

- `SparkFnRegistrar` registers `UDF1`/`UDF2` functions for similarity and hash
  calculations.
- `SparkBlockingTreeUtil` uses typed `Dataset.map` and a serialized blocking
  tree, which is opaque to Photon and Gluten/Velox.
- `SparkGraphUtil` delegates cluster consolidation to GraphFrames connected
  components/GraphX.
- `Matcher` builds graph-based match output after model scoring.
- `Trainer` persists a learned model and blocking tree through JVM model
  helpers rather than a declarative Spark relation.
- `TrainingDataFinder` creates self-joins and candidate pairs; this portion is
  the strongest first native rewrite candidate because it can be represented
  with joins, projections, filters, and window functions.

The first full native E2E target will therefore use the upstream phase
contracts and a declarative exact-similarity/candidate-pair path, with model
and graph substitutions documented as adapted semantics until parity tests
prove equivalence.
