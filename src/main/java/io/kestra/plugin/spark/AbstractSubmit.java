package io.kestra.plugin.spark;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.AbstractBash;
import io.kestra.core.tasks.scripts.ScriptOutput;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSubmit extends AbstractBash implements RunnableTask<ScriptOutput> {
    @Schema(
        title = "the Spark master hostname for the application.",
        description = "[](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls )"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String master;

    @Schema(
        title = "the application name."
    )
    @PluginProperty(dynamic = true)
    private String name;

    @Schema(
        title = "command line arguments for the application."
    )
    @PluginProperty(dynamic = true)
    private List<String> args;

    @Schema(
        title = "Adds a file to be submitted with the application.",
        description = "Must be Kestra internal storage url"
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> appFiles;

    @Schema(
        title = "Enables verbose reporting"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean verbose = false;

    @Schema(
        title = "configuration value for the application."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> configurations;


    @Schema(
        title = "command line arguments for the application."
    )
    @PluginProperty(dynamic = true)
    private DeployMode deployMode;

    @Schema(
        title = "the `spark-submit` binary path."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String sparkSubmitPath = "spark-submit";

    protected List<String> finalCommandsWithInterpreter(String commandAsString) throws IOException {
        return List.of(commandAsString);
    }

    abstract protected void configure(RunContext runContext, SparkLauncher spark) throws Exception;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        return run(runContext, throwSupplier(() -> {
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

            List<String> commands = new ArrayList<>();
            commands.add(this.sparkSubmitPath);
            commands.addAll(((KestraSparkLauncher) spark).getCommands());

            return String.join(" ", commands);
        }));
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
        File file = runContext.tempDir().resolve(runContext.render(name)).toFile();

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
