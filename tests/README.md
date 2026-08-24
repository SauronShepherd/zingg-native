# Validation

The previous prototype tests were moved to `reference/legacy-prototype-tests/` because they target the abandoned facade/custom-Connect-plugin architecture.

The executable tests in this directory validate repository/release contracts
locally. Databricks runtime evidence is generated separately by the
Serverless bundle jobs using the `sda` CLI profile.
