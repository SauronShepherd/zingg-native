# Dedicated Photon implementation

Dedicated runs the real patched Zingg JVM main class directly. The native-core JAR is installed beside the patched Zingg JAR and the integration overlay calls `NativeOperationProvider` at the Spark construction boundaries.

The shared rewrite registry emits public DataFrame expressions. Classic/Py4J support exists only as transport/control plumbing; rules do not depend on private `_jdf`/Catalyst mutation. Native mode defaults to STRICT in the provider unless overridden.
