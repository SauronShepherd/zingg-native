# Similarity implementation status

All similarity families on the standard pinned Zingg 0.7 feature path have source implementations in the public-expression registry, including Jaro and affine-gap/Monge-Elkan. They are marked `implemented-unvalidated`, not certified.

`BigramJaccSimFn` and other source-present/non-standard functions are intentionally fail-closed unless they are promoted into the supported execution inventory with an exact rewrite. No approximation aliases are used.

Databricks Serverless environment 5 controlled probes of the exact public
AffineGap expression completed successfully on 40 rows: 4-character inputs
in 11.2 seconds, 12-character inputs in 14.3 seconds, and 24-character inputs
in 31.6 seconds of Spark execution. These measurements show nonlinear growth
of the higher-order SQL recurrence and explain why the full 11-field train
remains a performance gate. The exact recurrence is retained for semantic
parity; it is not replaced with a different distance function.
