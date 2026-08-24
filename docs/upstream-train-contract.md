# Upstream train contract

Training remains orchestrated by upstream Zingg 0.7.0. In native mode, the
adapter replaces the Spark-specific model execution boundary (assembler,
polynomial expansion, logistic/CV fitting, prediction, and persistence) with
the public-DataFrame native model engine; it does not replace Zingg's training
phase contract, feature definitions, training-evidence rules, or model
orchestration.

The execution substitutions reached during training are handled at the shared Spark boundaries: feature similarities, blocking hashes/tree application, stop-word preprocessing, and vector extraction. This preserves the real Zingg training contract rather than introducing a second trainer.
