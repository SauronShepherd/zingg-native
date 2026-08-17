# Upstream `match` and `link` contracts

The pinned Zingg 0.7.0 executors show that matching is a multi-stage workflow:

- preprocess the input and select field-definition columns;
- apply the persisted blocking tree;
- build self-pairs from block membership;
- load the persisted model and predict pair scores;
- apply the prediction filter;
- construct graph-based output and consolidate connected components.

`link` changes more than the method name. It uses source-sensitive pair
construction, retains source information in the output, and uses a distinct
`LinkOutputBuilder`; it must not be an alias for self-matching.

The native adapter therefore cannot certify the current Python `match`/`link`
aliases or dense-rank clustering as Zingg parity. A compliant implementation
needs versioned model/blocking artifacts, source-aware pair relations, a
declarative prediction filter, and a documented replacement for graph
component consolidation before these phases enter the SAFE API.
