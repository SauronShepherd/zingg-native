# Usage model

`zingg-native` is transparent: keep the ordinary Zingg 0.7 application/CLI entry point. Apply the integration overlay when building Zingg, place the native-core JAR next to the patched Zingg JAR, and run with native mode enabled.

Dedicated Photon launches the real Zingg main class directly. Serverless uses `ai.zingg.native.launch.DatabricksZinggMain` only to create the managed `DatabricksSession`, then reflectively delegates the original arguments to the real Zingg main class.

The repository intentionally does not provide a second `train/match/link` API.
