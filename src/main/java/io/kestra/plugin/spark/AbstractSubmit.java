package io.kestra.plugin.spark;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
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
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

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
    @PluginProperty(dynamic = true)
    @NotNull
    private String master;

    @Schema(
        title = "Spark application name."
    )
    @PluginProperty(dynamic = true)
    private String name;

    @Schema(
        title = "Command line arguments for the application."
    )
    @PluginProperty(dynamic = true)
    private List<String> args;

    @Schema(
        title = "Adds a file to be submitted with the application.",
        description = "Must be an internal storage URI."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> appFiles;

    @Schema(
        title = "Enables verbose reporting."
    )
    @PluginProperty
    @Builder.Default
    private Boolean verbose = false;

    @Schema(
        title = "Configuration properties for the application."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> configurations;


    @Schema(
        title = "Deploy mode for the application."
    )
    @PluginProperty(dynamic = true)
    private DeployMode deployMode;

    @Schema(
        title = "The `spark-submit` binary path."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String sparkSubmitPath = "spark-submit";

    @Schema(
        title = "Additional environment variables for the current process."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> env;

    @Builder.Default
    @Schema(
        title = "Script runner to use."
    )
    @PluginProperty
    @NotNull
    @NotEmpty
    protected RunnerType runner = RunnerType.PROCESS;

    @Schema(
        title = "Docker options when using the `DOCKER` runner.",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    protected DockerOptions docker = DockerOptions.builder().build();

    abstract protected void configure(RunContext runContext, SparkLauncher spark) throws Exception;

    protected DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        SparkLauncher spark = new KestraSparkLauncher(this.envs(runContext))
            .setMaster(runContext.render(master))
            .setVerbose(this.verbose);

        if (this.name != null) {
            spark.setAppName(runContext.render(this.name));
        }

        if (this.configurations != null) {
            this.configurations.forEach(throwBiConsumer((key, value) ->
                spark.setConf(runContext.render(key), runContext.render(value))
            ));
        }

        if (this.args != null) {
            runContext.render(this.args).forEach(throwConsumer(spark::addAppArgs));
        }

        if (this.appFiles != null) {
            this.appFiles.forEach(throwBiConsumer((key, value) -> spark.addFile(this.tempFile(runContext, key, value))));
        }

        this.configure(runContext, spark);

        List<String> commandsArgs = new ArrayList<>();
        commandsArgs.add(this.sparkSubmitPath);
        commandsArgs.addAll(((KestraSparkLauncher) spark).getCommands());

        return new CommandsWrapper(runContext)
            .withEnv(this.envs(runContext))
            .withRunnerType(this.runner)
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withCommands(ScriptService.scriptCommands(
                List.of("/bin/sh", "-c"),
                List.of(),
                String.join(" ", commandsArgs)
            ))
            .run();
    }

    private Map<String, String> envs(RunContext runContext) throws IllegalVariableEvaluationException {
        HashMap<String, String> result = new HashMap<>();

        if (this.env != null) {
            this.env.forEach(throwBiConsumer((s, s2) -> {
                result.put(runContext.render(s), runContext.render(s2));
            }));
        }

        return result;
    }

    protected String tempFile(RunContext runContext, String name, String url) throws IOException, IllegalVariableEvaluationException, URISyntaxException {
        File file = runContext.resolve(Path.of(runContext.render(name))).toFile();

        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            URI from = new URI(runContext.render(url));
            IOUtils.copyLarge(runContext.uriToInputStream(from), fileOutputStream);

            return file.getAbsoluteFile().toString();
        }
    }

    public enum DeployMode {
        CLIENT,
        CLUSTER,
    }
}
