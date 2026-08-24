package ai.zingg.nativebridge.launch;

/**
 * Java static-main entry point for Databricks JAR-task validation.
 * Databricks resolves the class before launching the Serverless environment;
 * keeping this wrapper Java avoids Scala-object main-class ambiguity while
 * preserving the single Scala launcher implementation.
 */
public final class DatabricksZinggMainEntry {
    private DatabricksZinggMainEntry() {}

    public static void main(String[] args) {
        try {
            Class<?> moduleClass = Class.forName("ai.zingg.native.launch.DatabricksZinggMain$");
            Object module = moduleClass.getField("MODULE$").get(null);
            moduleClass.getMethod("main", String[].class).invoke(module, (Object) args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to invoke Databricks native launcher", e);
        }
    }
}
