# Architecture

The facade accepts a Classic or Connect Spark session and builds standard Spark
DataFrame expressions. The current operation registry is intentionally small:
`Zingg.exact()` delegates to a transport-neutral expression backend. No private
JVM handles, UDFs, custom physical operators, or Catalyst rules are involved.

This architecture is native-engine friendly by construction: the resulting
logical plan is visible to Spark's optimizer and physical planner. A future JVM
core and Connect extension can be added behind the same backend boundary when a
concrete operation requires server-side logic.
