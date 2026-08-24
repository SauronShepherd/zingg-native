# Development and build entry points

This repository separates implementation from validation. The commands below are build/deployment entry points for a later run; they were not executed while producing this revision.

```text
# public Spark 4.0 core
mvn -Pspark40 package -DskipTests

# public Spark 4.1 core
mvn -Pspark41 package -DskipTests

# Dedicated runtime profiles
mvn -Pdatabricks-dedicated-17.3 package -DskipTests
mvn -Pdatabricks-dedicated-18-lts package -DskipTests

# Serverless core + launcher
mvn -Pdatabricks-serverless-env5 package -DskipTests
```

Apply the Zingg integration with:

```text
The overlay and native integration sources live in this repository. Do not point the build at or modify a Zingg working tree.
```

`integration/zingg-native-core-dependency.xml` is reference metadata for an external deployment assembly only; this project never injects it into or edits that assembly.

The default Maven reactor contains only `core`. `serverless-launcher` is activated by the Serverless profile. Historical custom-Connect-plugin code is archived under `reference/` and is not buildable from the production reactor.
