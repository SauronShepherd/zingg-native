# Spark 4.x build strategy

The root POM provides Spark 4.0 and 4.1 profiles and uses Scala 2.13/JDK 17 for this adapter line. Spark runtime dependencies are `provided`; the adapter must not bundle an Apache Spark distribution into Databricks runtime JARs.

Zingg itself must be built from its Spark 4.x-capable branch/profile after applying the integration overlay. The overlay intentionally imports only the native provider's public API and avoids Catalyst/GraphFrames compile-time dependencies in rewrite mode.
