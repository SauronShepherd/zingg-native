# Semantic contracts

The contract for every rewrite is the corresponding pinned Zingg 0.7.0 implementation, including null/blank conventions and Java/SecondString edge behavior. See `semantic-parity.md` and `rewrite-rules.md`.

The adapter must never substitute a merely similar metric in STRICT mode. If a semantic operation cannot be represented exactly by a registered rule, the correct behavior is an explicit native-rewrite failure.

## Internal row-ID contract

The native `_native_row_id` is an opaque, non-null, unique row key used only to
reassemble chunked feature columns and preserve the input-row association. It
is not a published Zingg `z_zid`, is not required to be stable across separate
actions or jobs, and must never be exposed as a user-facing cluster identifier.
The rewrite therefore preserves the required invariant (unique association
within the action) without claiming equality with upstream `zipWithUniqueId`
values. Published cluster IDs and output schema remain owned by ordinary Zingg
phase logic; the local contract test checks the public-expression implementation
and the Serverless ordinary-phase runs exercise the resulting association.
