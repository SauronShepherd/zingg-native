# Release readiness

Implementation is present. Databricks Serverless environment 5 has verified
the real Zingg 0.7.0 launch contract and a 15-rule, 18-row STRICT differential probe
against the pinned reference. The source contains the transparent Zingg 0.7
integration, public-expression rule registry, blocking/tree and graph
replacements, strict failure policy, Dedicated/Serverless launch paths,
packaging profiles, capability metadata, and evidence hooks.

Do not call the result production-ready or Photon-certified yet. Dedicated
Photon execution, semantic differential coverage, all certified phases, and
authoritative Databricks Query Profile/operator evidence remain release gates.
No legacy prototype evidence counts for this revision.

The full-feature Serverless train gate is evidenced on the current release
line (the authoritative production run used release 20260823-assembly-boundary1; the currently deployed regression line is
20260823-blocking-family2). Production validation completed the full 20-feature,
1,770-term initial fit plus five-reg/two-fold cross-validation grid, saved the
native model, and terminated SUCCESS. An independent soak run completed the
same 1,100 aggregate path. Bounded
`train`, `label`, `updateLabel`, `match`, and `link` phases
are evidenced in `docs/evidence/databricks-serverless-v5.json`. Current-release
model fit/save/load/predict and separate cross-job load/predict also passed,
including the full 1,770-term shape.

A clean bounded fixture has now completed ordinary Zingg `train`, `label`,
`updateLabel`, `match`, and `link` on Serverless; this is phase-level bounded evidence, not full-scale
production-grid certification.

The materialization recovery gate is also runtime-proven: an expected failed
Serverless job writes a UUID-scoped transient sentinel, and a separate job
verifies that the transient root is absent afterward. This does not imply that
published model directories or ordinary Zingg outputs are deleted.
