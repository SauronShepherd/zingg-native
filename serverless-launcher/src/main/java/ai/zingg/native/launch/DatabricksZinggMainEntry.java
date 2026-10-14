package ai.zingg.nativebridge.launch;

import java.util.ArrayList;
import java.util.List;

/**
 * Java static-main entry point for Databricks JAR-task validation.
 * Databricks resolves the class before launching the Serverless environment;
 * keeping this wrapper Java avoids Scala-object main-class ambiguity while
 * preserving the single Scala launcher implementation.
 */
public final class DatabricksZinggMainEntry {
    private static final String NATIVE_EXPLAIN_FLAG = "--native-explain";

    private DatabricksZinggMainEntry() {}

    public static void main(String[] args) {
        String[] forwarded = configureNativeExplain(args);
        try {
            Class<?> moduleClass = Class.forName("ai.zingg.native.launch.DatabricksZinggMain$");
            Object module = moduleClass.getField("MODULE$").get(null);
            moduleClass.getMethod("main", String[].class).invoke(module, (Object) forwarded);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to invoke Databricks native launcher", e);
        }
    }

    static String[] configureNativeExplain(String[] args) {
        List<String> forwarded = new ArrayList<>(args.length);
        for (String arg : args) {
            if (NATIVE_EXPLAIN_FLAG.equals(arg)) {
                System.setProperty("zingg.native.explain", "true");
            } else {
                forwarded.add(arg);
            }
        }
        return forwarded.toArray(String[]::new);
    }
}
