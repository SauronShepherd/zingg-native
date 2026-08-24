# Rewrite registry

The authoritative implementation is `PublicRewriteRules.scala`; `NativeOperationProvider` maps the concrete upstream semantic function class or hash-function name to that registry.

Implemented similarity families: exact string/value, null/blank checks, integer/long/float/double/date, array-double cosine, Jaccard, numeric Jaccard, product code, SecondString-compatible Jaro, affine-gap/Monge-Elkan, email, PIN, alphabet-only exact/fuzzy, and same-first-word.

Implemented blocking/hash families: first/last UTF-16 characters, last word, null/empty, identity, first-character boxes, less-than-zero, decimal truncation, last-digit trimming, numeric ranges, and Java-compatible round.

Implemented miscellaneous rules: trim, case normalization, stop-word removal, ML probability-vector extraction, blocking-tree compilation, graph connected components, and the native `SparkModel` polynomial/logistic-CV/prediction/persistence boundary.

Unknown or explicitly disabled rules fail in STRICT mode. Source-present functions classified as non-standard/unreachable are not silently substituted.
