package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run Spark CLI commands",
    description = "Executes provided Spark CLI lines (e.g., spark-submit, spark-sql) inside the task runner. Prepends /opt/spark/bin to PATH when missing and streams Spark logs as info. Defaults to container image apache/spark:4.0.1-java21-r."
)
@Plugin(
    examples = {
        @Example(
            title = "Submit a PySpark job to a master node.",
            full = true,
            code = """
            id: spark_cli
            namespace: company.team

            tasks:
              - id: hello
                type: io.kestra.plugin.spark.SparkCLI
                inputFiles:
                  pi.py: |
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
                taskRunner:
                  type: io.kestra.plugin.scripts.runner.docker.Docker
                  networkMode: host
                commands:
                  - spark-submit --name Pi --master spark://localhost:7077 pi.py"""
        )
    }
)
public class SparkCLI extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "apache/spark:4.0.1-java21-r";

    @Schema(
        title = "CLI commands to execute",
        description = "Ordered list of Spark CLI invocations run with the configured interpreter; required."
    )
    @NotNull
    private Property<List<String>> commands;

    @Schema(
        title = "Container image for task runner",
        description = "Applies when the task runner is container-based; defaults to `apache/spark:4.0.1-java21-r`."
    )
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var rBeforeCommands = runContext.render(this.beforeCommands).asList(String.class);

        if (rBeforeCommands.stream().noneMatch(c -> c.contains("export PATH")))
            rBeforeCommands = Stream.concat(Stream.of("export PATH=$PATH:/opt/spark/bin"), rBeforeCommands.stream()).toList();

        return this.commands(runContext)
            // spark set all logs in stdErr so we force all logs on info
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String line, Boolean isStdErr, Instant instant) {
                    runContext.logger().info(line);
                }
                @Override
                public void accept(String line, Boolean aBoolean) {
                    runContext.logger().info(line);
                }
            })
            .withInterpreter(this.interpreter)
            .withBeforeCommands(Property.ofValue(rBeforeCommands))
            .withBeforeCommandsWithOptions(true)
            .withCommands(this.commands)
            .run();
    }
}
