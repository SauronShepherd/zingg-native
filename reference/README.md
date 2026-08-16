# Semantic reference

`zingg-0.7.0.lock` pins the upstream semantic reference used by parity tests.
The production package must not depend on the upstream AGPL artifact merely to
run; reference code belongs in test tooling and must be checked out at the
locked commit before generating goldens.

Run `scripts/prepare-reference.ps1` to create the detached pinned checkout.
`scripts/check-reference.ps1` refuses to run the oracle when the checkout is
dirty or points at another commit.
