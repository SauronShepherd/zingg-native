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
