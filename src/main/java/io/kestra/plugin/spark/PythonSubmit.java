package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkLauncher;

import java.io.FileWriter;
import java.nio.file.Path;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit a PySpark job to a remote cluster."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "runner: DOCKER",
                "docker:",
                "  networkMode: host",
                "  user: root",
                "master: spark://localhost:7077",
                "args:",
                "- \"10\"",
                "mainScript: |",
                "  import sys",
                "  from random import random",
                "  from operator import add",
                "  from pyspark.sql import SparkSession",
                "",
                "",
                "  if __name__ == \"__main__\":",
                "      spark = SparkSession \\",
                "          .builder \\",
                "          .appName(\"PythonPi\") \\",
                "          .getOrCreate()",
                "",
                "      partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2",
                "      n = 100000 * partitions",
                "",
                "      def f(_: int) -> float:",
                "          x = random() * 2 - 1",
                "          y = random() * 2 - 1",
                "          return 1 if x ** 2 + y ** 2 <= 1 else 0",
                "",
                "      count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)",
                "      print(\"Pi is roughly %f\" % (4.0 * count / n))",
                "",
                "      spark.stop()",
            }
        )
    }
)
public class PythonSubmit extends AbstractSubmit {
    @Schema(
        title = "The main Python script."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainScript;

    @Schema(
        title = "Adds a Python file/zip/egg package to be submitted with the application.",
        description = "Must be an internal storage URI."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> pythonFiles;

    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        Path path = runContext.tempFile(".py");
        try (FileWriter fileWriter = new FileWriter(path.toFile())) {
            IOUtils.write(runContext.render(this.mainScript), fileWriter);
            fileWriter.flush();
        }

        spark.setAppResource("file://" + path.toFile().getAbsolutePath());

        if (this.pythonFiles != null) {
            this.pythonFiles.forEach(throwBiConsumer((key, value) -> spark.addPyFile(this.tempFile(runContext, key, value))));
        }
    }
}
