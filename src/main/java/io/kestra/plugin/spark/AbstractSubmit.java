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
    private static final String DEFAULT_IMAGE = "bitnami/spark";

    @Schema(
        title = "Spark master hostname for the application.",
        description = "Spark master URL [formats](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)."
    )
    @NotNull
    private Property<String> master;

    @Schema(
        title = "Spark application name."
    )
    private Property<String> name;

    @Schema(
        title = "Command line arguments for the application."
    )
    private Property<List<String>> args;

    @Schema(
        title = "Adds a file to be submitted with the application.",
        description = "Must be an internal storage URI."
    )
    private Property<Map<String, String>> appFiles;

    @Schema(
        title = "Enables verbose reporting."
    )
    @Builder.Default
    private Property<Boolean> verbose = Property.of(false);

    @Schema(
        title = "Configuration properties for the application."
    )
    private Property<Map<String, String>> configurations;


    @Schema(
        title = "Deploy mode for the application."
    )
    private Property<DeployMode> deployMode;

    @Schema(
        title = "The `spark-submit` binary path."
    )
    @Builder.Default
    private Property<String> sparkSubmitPath = Property.of("spark-submit");

    @Schema(
        title = "Additional environment variables for the current process."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Script runner to use.",
        description = "Deprecated - use 'taskRunner' instead."
    )
    protected Property<RunnerType> runner;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
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
            .setVerbose(runContext.render(verbose).as(Boolean.class).orElseThrow());

        if (this.name != null) {
            spark.setAppName(runContext.render(this.name).as(String.class).orElseThrow());
        }

        if (this.configurations != null) {
            runContext.render(this.configurations).asMap(String.class, String.class)
                .forEach(throwBiConsumer(spark::setConf));
        }

        if (this.args != null) {
            runContext.render(this.args).asMap(String.class, String.class)
                .forEach(throwConsumer(spark::addAppArgs));
        }

        if (this.appFiles != null) {
            runContext.render(this.appFiles).asMap(String.class, String.class)
                .forEach(throwBiConsumer((key, val) -> spark.addFile(this.tempFile(runContext, key, val))));
        }

        this.configure(runContext, spark);

        List<String> commandsArgs = new ArrayList<>();
        commandsArgs.add(runContext.render(this.sparkSubmitPath).as(String.class).orElseThrow());
        commandsArgs.addAll(((KestraSparkLauncher) spark).getCommands());

        return new CommandsWrapper(runContext)
            .withEnv(this.envs(runContext))
            .withRunnerType(runContext.render(this.runner).as(RunnerType.class).orElseThrow())
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(ScriptService.scriptCommands(
                List.of("/bin/sh", "-c"),
                List.of(),
                String.join(" ", commandsArgs)
            ))
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
        CLIENT,
        CLUSTER,
    }
}
