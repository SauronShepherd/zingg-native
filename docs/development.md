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
.venv\Scripts\python -m pytest -q
mvn test
```

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
