package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.AbstractLogConsumer;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute Spark CLI commands."
)
@Plugin(
    examples = {
        @Example(
            title = "Submit a PySpark job to a master node.",
            full = true,
            code = """
                id: spark-cli
                namespace: dev
                tasks:
                  - id: workingDirectory
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                    - id: local-file
                      type: io.kestra.core.tasks.storages.LocalFiles
                      inputs:
                        pi.py: |
                          import sys
                          from random import random
                          from operator import add
                          from pyspark.sql import SparkSession
                
                
                          if __name__ == "__main__":
                              spark = SparkSession \\
                                  .builder \\
                                  .appName("PythonPi") \\
                                  .getOrCreate()
                
                              partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
                              n = 100000 * partitions
                
                              def f(_: int) -> float:
                                  x = random() * 2 - 1
                                  y = random() * 2 - 1
                                  return 1 if x ** 2 + y ** 2 <= 1 else 0
                
                              count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
                              print("Pi is roughly %f" % (4.0 * count / n))
                
                              spark.stop()
                    - id: hello
                      type: io.kestra.plugin.spark.SparkCLI
                      docker:
                        networkMode: host
                      commands:
                        - spark-submit --name Pi --master spark://localhost:7077 pi.py"""
        )
    }
)
public class SparkCLI extends AbstractExecScript {
    private static final String DEFAULT_IMAGE = "bitnami/spark";

    @Schema(
        title = "The list of Spark CLI commands to run."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private List<String> commands;

    @Schema(
        title = "Docker options for the `DOCKER` runner.",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    private DockerOptions docker = DockerOptions.builder().build();

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> commandsArgs = ScriptService.scriptCommands(
            this.interpreter,
            this.beforeCommands,
            this.commands
        );

        return this.commands(runContext)
            // spark set all logs in stdErr so we force all logs on info
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean aBoolean) {
                    runContext.logger().info(line);
                }
            })
            .withCommands(commandsArgs)
            .run();
    }
}
