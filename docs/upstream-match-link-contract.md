# Upstream match/link contract

`match` and `link` remain the real Zingg 0.7.0 workflows, including their different pair/source semantics and output builders. The adapter does not alias or reimplement them.

Their non-native Spark boundaries are intercepted underneath the phase orchestration: preprocessing, blocking tree/hashes, feature similarities, model vector extraction, and graph connected-components consolidation.
