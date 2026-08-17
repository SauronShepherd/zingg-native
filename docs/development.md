# Development

Requirements:

- JDK 17 for the supported JVM build;
- Maven 3.9 or newer;
- Python 3.10+;
- Spark 4.1 for integration tests.

The Python package intentionally does not install PySpark into a production
runtime. For local tests use a dedicated environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\python -m pip install pyspark==4.1.0
$env:PYTHONPATH = (Resolve-Path .\python\src)
.venv\Scripts\python -m pytest -q
./scripts/build.ps1
```

Setting `PYTHONPATH` ensures tests import the checked-out source rather than a
previously installed wheel with the same package name.

The build script uses `MAVEN_HOME` when configured, then the repository's
ignored `.tools/apache-maven-3.9.11` runtime, and finally `mvn` from `PATH`.

Keep Databricks Connect in a separate environment from OSS PySpark. The
Connect server plugin and the Classic core JAR are compiled by Maven and must
be tested against the Spark API version declared by the selected runtime.

## Classic candidate-phase E2E

After building the JVM artifacts, the first shared-core phase can be verified
through the real Py4J boundary with:

```powershell
$env:ZINGG_NATIVE_CORE_JAR = (Resolve-Path .\core\target\zingg-native-core_2.13-0.2.0-SNAPSHOT.jar)
python .\examples\classic_candidate_e2e.py
```

The example asserts the Classic handshake, native candidate relation schema,
exact-key scores, and nullable training labels. It is local Classic evidence,
not Databricks or Spark Connect evidence.

Classic phase methods also accept `output_path` and persist through the shared
Scala gateway using Parquet. On Windows, running a local persistence check
requires a Hadoop Windows utility (`HADOOP_HOME\bin\winutils.exe`); Serverless
and Linux environments do not have that Windows prerequisite.

To produce release integrity hashes after building all artifacts:

```powershell
python scripts/artifact-checksums.py
```

CI publishes the same three-line SHA-256 manifest as a workflow artifact.
# Development gates

Run the architecture boundary check before submitting changes:

```powershell
./scripts/check-architecture.ps1
```

It rejects private Spark JVM handles outside the approved transport modules and
ensures the Python similarity module remains explicitly marked as a comparison
prototype. CI runs the same check on every Python build.
