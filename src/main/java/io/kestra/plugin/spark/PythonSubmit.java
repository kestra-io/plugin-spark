package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    title = "Submit PySpark job to Spark",
    description = "Writes the provided Python script to a temp file, uploads referenced assets, then calls spark-submit against the configured master."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: spark_python_submit
                namespace: company.team

                tasks:
                  - id: python_submit
                    type: io.kestra.plugin.spark.PythonSubmit
                    taskRunner:
                      type: io.kestra.plugin.scripts.runner.docker.Docker
                      networkMode: host
                      user: root
                    master: spark://localhost:7077
                    args:
                      - "10"
                    mainScript: |
                      import sys
                      from random import random
                      from operator import add
                      from pyspark.sql import SparkSession


                      if __name__ == "__main__":
                          spark = SparkSession.builder.appName("PythonPi").getOrCreate()

                          partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
                          n = 100000 * partitions

                          def f(_: int) -> float:
                              x = random() * 2 - 1
                              y = random() * 2 - 1
                              return 1 if x ** 2 + y ** 2 <= 1 else 0

                          count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
                          print("Pi is roughly %f" % (4.0 * count / n))

                          spark.stop()
                """
        )
    }
)
public class PythonSubmit extends AbstractSubmit {
    @Schema(
        title = "Main Python script content",
        description = "Inline script body written to a temporary .py file and used as the application resource."
    )
    @NotNull
    private Property<String> mainScript;

    @Schema(
        title = "Additional Python files or archives",
        description = "Map of filenames to internal storage URIs passed through `--py-files`."
    )
    private Property<Map<String, String>> pythonFiles;

    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        Path path = runContext.workingDir().createTempFile(".py");
        try (FileWriter fileWriter = new FileWriter(path.toFile())) {
            IOUtils.write(runContext.render(this.mainScript).as(String.class).orElseThrow(), fileWriter);
            fileWriter.flush();
        }

        spark.setAppResource("file://" + path.toFile().getAbsolutePath());

        runContext.render(this.pythonFiles).asMap(String.class, String.class)
            .forEach(throwBiConsumer((key, value) -> spark.addPyFile(this.tempFile(runContext, key, value))));

    }
}
