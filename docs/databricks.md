# Databricks support boundary

The Databricks target for this project is Databricks Serverless. Serverless
exposes Spark through Spark Connect, so the release path requires the native
Connect server plugin to be installed and loaded in the managed environment.
Dedicated/Photon validation is out of scope for this project unless explicitly
requested again.

Databricks Serverless remains unclaimed. The previous wheel-only Serverless
runs exercised Python-built Spark expressions through Spark Connect; they did
not install or execute this repository's Connect server plugin and therefore
cannot prove the shared-core architecture.

A real Serverless JAR-task run now verifies the shared Scala core path:

- job `295665184144562`, latest run `923271170978947`;
- Serverless Spark `4.1.0`, task output
  `ZINGG_NATIVE_SERVERLESS_CORE_E2E PASS similarities=exact,jaccard,jaro phases=findTrainingData,label,updateLabel spark=4.1.0`;
- reproducible definition: `databricks-serverless-core-e2e.json`.

The corresponding task run is `879126116901332`. This is shared-core JAR
evidence only. It is not evidence that the managed
Spark Connect `ExpressionPlugin` is installed or active.

The repository's historical `databricks-e2e-job*.json` definitions are retained
only for provenance and are explicitly marked `DISABLED` and
`prototype-only`; they must not be submitted as native release tests.

Required evidence before a Databricks release claim:

- core and Connect JARs installed on the target compute;
- gateway/plugin handshake reports protocol v1 and the expected core version;
- the same Exact/Jaccard outputs are observed through the claimed transport;
- Serverless plugin installation and loading are demonstrated on the actual
  managed environment;
- the same Exact/Jaccard outputs are observed through that managed Connect
  transport;
- unsupported plugin deployment is recorded rather than inferred.
