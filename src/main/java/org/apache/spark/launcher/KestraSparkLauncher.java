package org.apache.spark.launcher;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.isWindows;
import static org.apache.spark.launcher.CommandBuilderUtils.join;

public class KestraSparkLauncher extends SparkLauncher {
    public KestraSparkLauncher(Map<String, String> env) {
        super(env);
    }

    public List<String> getCommands() {
        return builder.buildSparkSubmitArgs();
    }

    String findSparkSubmit() {
        String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
        return join(File.separator, builder.getSparkHome(), "bin", script);
    }
}
