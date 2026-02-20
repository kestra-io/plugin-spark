package io.kestra.plugin.spark;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.KestraSparkLauncher;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSubmit extends Task implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "apache/spark:4.0.1-java21-r";

    @Schema(
        title = "Set Spark master endpoint",
        description = "Required Spark master URL (e.g., `spark://host:port`, `local[*]`). Must follow Spark master URL formats."
    )
    @NotNull
    private Property<String> master;

    @Schema(
        title = "Name the Spark application",
        description = "Optional application name passed to Spark; falls back to Spark defaults when empty."
    )
    private Property<String> name;

    @Schema(
        title = "Pass arguments to application",
        description = "Command-line arguments forwarded to the application in order."
    )
    private Property<List<String>> args;

    @Schema(
        title = "Ship additional files with job",
        description = "Map of local filenames to internal storage URIs; each file is downloaded to the working directory and sent with --files."
    )
    private Property<Map<String, String>> appFiles;

    @Schema(
        title = "Enable verbose spark-submit output",
        description = "Defaults to false; sets --verbose for detailed submission logs."
    )
    @Builder.Default
    private Property<Boolean> verbose = Property.ofValue(false);

    @Schema(
        title = "Spark configuration overrides",
        description = "Key/value Spark configurations applied via --conf before submission."
    )
    private Property<Map<String, String>> configurations;


    @Schema(
        title = "Choose Spark deploy mode",
        description = "client or cluster; defaults to client when unset."
    )
    @Builder.Default
    private Property<DeployMode> deployMode = Property.ofValue(DeployMode.CLIENT);

    @Schema(
        title = "Path to spark-submit binary",
        description = "Absolute path to spark-submit; defaults to `/opt/spark/bin/spark-submit`."
    )
    @Builder.Default
    private Property<String> sparkSubmitPath = Property.ofValue("/opt/spark/bin/spark-submit");

    @Schema(
        title = "Environment variables for spark-submit",
        description = "Rendered key/value pairs added to the submission process environment."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Execution engine (deprecated)",
        description = "Deprecated; use taskRunner instead. Defaults to RunnerType.DOCKER when specified."
    )
    protected Property<RunnerType> runner;

    @Schema(
        title = "Docker runner options (deprecated)",
        description = "Deprecated in favor of taskRunner; only applied when using the legacy runner property."
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner implementation",
        description = "Runner definition (e.g., Docker). Defaults to the Docker task runner; each runner exposes its own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Container image for task runner",
        description = "Used when the task runner is container-based; defaults to `apache/spark:4.0.1-java21-r`."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    abstract protected void configure(RunContext runContext, SparkLauncher spark) throws Exception;

    protected DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        SparkLauncher spark = new KestraSparkLauncher(this.envs(runContext))
            .setMaster(runContext.render(master).as(String.class).orElseThrow())
            .setVerbose(runContext.render(verbose).as(Boolean.class).orElse(false));

        if (this.name != null) {
            spark.setAppName(runContext.render(this.name).as(String.class).orElseThrow());
        }

        if (this.configurations != null) {
            runContext.render(this.configurations).asMap(String.class, String.class)
                .forEach(throwBiConsumer(spark::setConf));
        }

        if (this.args != null) {
            runContext.render(this.args).asList(String.class)
                .forEach(throwConsumer(spark::addAppArgs));
        }

        if (this.appFiles != null) {
            runContext.render(this.appFiles).asMap(String.class, String.class)
                .forEach(throwBiConsumer((key, val) -> spark.addFile(this.tempFile(runContext, key, val))));
        }

        if (this.deployMode != null) {
            spark.setDeployMode(runContext.render(this.deployMode).as(DeployMode.class).orElse(DeployMode.CLIENT).value());
        }

        runContext.logger().info(runContext.render(this.deployMode).as(DeployMode.class).orElse(DeployMode.CLIENT).value());

        this.configure(runContext, spark);

        List<String> commandsArgs = new ArrayList<>();
        commandsArgs.add(runContext.render(this.sparkSubmitPath).as(String.class).orElse("spark-submit"));
        commandsArgs.addAll(((KestraSparkLauncher) spark).getCommands());

        return new CommandsWrapper(runContext)
            .withEnv(this.envs(runContext))
            .withRunnerType(runContext.render(this.runner).as(RunnerType.class).orElse(RunnerType.DOCKER))
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(List.of(String.join(" ", commandsArgs))))
            .run();
    }

    private Map<String, String> envs(RunContext runContext) throws IllegalVariableEvaluationException {
        HashMap<String, String> result = new HashMap<>();

        runContext.render(this.env).asMap(String.class, String.class)
            .forEach(throwBiConsumer((s, s2) -> {
                result.put(runContext.render(s), runContext.render(s2));
            }));

        return result;
    }

    protected String tempFile(RunContext runContext, String name, String url) throws IOException, URISyntaxException {
        File file = runContext.workingDir().resolve(Path.of(name)).toFile();

        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            URI from = new URI(url);
            IOUtils.copyLarge(runContext.storage().getFile(from), fileOutputStream);

            return file.getAbsoluteFile().toString();
        }
    }

    public enum DeployMode {
        CLIENT("client"),
        CLUSTER("cluster");

        private final String value;

        DeployMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        public String value() {
            return value;
        }
    }
}
