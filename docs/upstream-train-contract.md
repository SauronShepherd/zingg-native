# Upstream `train` contract

The pinned Zingg 0.7.0 source (`reference/upstream-zingg`, commit recorded in
`reference/zingg-0.7.0.lock`) shows that `SparkTrainer` is not a threshold
calculation over a labeled relation.

The executor contract is:

1. Read persisted training data and preprocess it.
2. Self-join by `z_cluster` and split pairs by `z_isMatch`.
3. Require at least five positive and five negative pairs.
4. Build a blocking tree from the test-data sample and positive pairs.
5. Persist the blocking tree.
6. Create a configured `Model`, fit it on positive and negative feature rows,
   and persist the learned model.

The model and blocking-tree artifacts are therefore part of the public phase
contract. A native implementation cannot substitute a scalar threshold or a
Python dictionary and claim Zingg parity. The shared-core implementation must
first define versioned, transport-independent model and blocking artifact
schemas, then provide declarative Spark feature construction and persistence.

The shared core now defines versioned `ModelArtifact` and
`BlockingTreeArtifact` contracts in `Artifacts.scala`. Their schema versions
are also exposed by the capability manifest; these contracts are validated but
do not persist or fit a model.

Current status: `train` remains unsupported in the SAFE API. The old Python
threshold model is retained only behind the explicitly selected prototype
backend and is not evidence of upstream parity.
