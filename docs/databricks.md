# Databricks support boundary

The Databricks release target is both Dedicated compute with Photon and
Serverless. Dedicated uses the Classic/JVM path where supported; Serverless
must use Databricks-supported public Spark APIs. The custom Spark Connect
server-plugin experiment is not a Serverless release dependency.

Databricks targets remain unclaimed until real Zingg differential and Photon
evidence is captured. The previous wheel-only Serverless
runs exercised Python-built Spark expressions through Spark Connect; they did
not install or execute this repository's Connect server plugin and therefore
cannot prove the shared-core architecture.

A real Serverless JAR-task run verifies the shared Scala core path:

- job `295665184144562`, latest verified run `847651604040137`;
- Serverless Spark `4.1.0`, Asset Bundle task output
  `ZINGG_NATIVE_SERVERLESS_CORE_E2E PASS similarities=exact,jaccard,jaro phases=preprocess,findTrainingData,buildTrainingPairs,label,updateLabel trainingEvidence=5/5 persistence=true spark=4.1.0 storage=/Volumes/sda_dev/sandbox/zingg_native_e2e/bundle-run`;
- reproducible definition: `databricks-serverless-core-e2e.json`.

The corresponding task run is `1077678349754978`. This is shared-core JAR
evidence only and is not yet real Zingg workflow parity or Photon evidence.

The phase outputs were persisted and reloaded from the managed Unity Catalog
volume `/Volumes/sda_dev/sandbox/zingg_native_e2e/run`. A separate test write
to `/tmp` was rejected with `DBFS_DISABLED`; `/tmp` is not a valid Serverless
evidence path.

The historical managed Connect feasibility test was executed as Serverless job
`177009162619307`, run `813057979287661`, task run `1075870166806203`. It was
rejected before native execution with Databricks error
`CONFIG_NOT_AVAILABLE: Configuration zingg.native.connect.plugin.loaded is not available`.
This is historical evidence that the old plugin-dependent adapter path was
unsupported; the target architecture no longer depends on that activation.

The repository's historical `databricks-e2e-job*.json` definitions are retained
only for provenance and are explicitly marked `DISABLED` and
`prototype-only`; they must not be submitted as native release tests.

Required evidence before a Databricks release claim:

- core and Connect JARs installed on the target compute;
- gateway/plugin handshake reports protocol v1 and the expected core version;
- the same Exact/Jaccard outputs are observed through the claimed transport;
- real Zingg 0.7 workflow parity is demonstrated on each claimed target;
- Photon and Serverless execution evidence is captured from supported runtime
  plan/query-profile mechanisms;
- unsupported plugin deployment is recorded rather than inferred.
