package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.spark.launcher.SparkLauncher;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit Spark job with JAR",
    description = "Uploads the provided application JAR and runs it via spark-submit on the target Spark master."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
            id: spark_jar_submit
            namespace: company.team

            inputs:
              - id: file
                type: FILE

            tasks:
              - id: jar_submit
                type: io.kestra.plugin.spark.JarSubmit
                taskRunner:
                    type: io.kestra.plugin.scripts.runner.docker.Docker
                    networkMode: host
                    user: root
                master: spark://localhost:7077
                mainResource: "{{ inputs.file }}"
                mainClass: spark.samples.App"""
        )
    }
)
public class JarSubmit extends AbstractSubmit {
    @Schema(
        title = "Application JAR resource",
        description = "Internal storage URI to the runnable application JAR uploaded to the working directory."
    )
    @NotNull
    private Property<String> mainResource;

    @Schema(
        title = "Application main class",
        description = "Fully qualified entrypoint class passed to spark-submit `--class`."
    )
    @NotNull
    private Property<String> mainClass;

    @Schema(
        title = "Additional dependency JARs",
        description = "Map of filenames to internal storage URIs added via `--jars`."
    )
    private Property<Map<String, String>> jars;

    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        String appJar = this.tempFile(runContext, "app.jar", runContext.render(this.mainResource).as(String.class).orElseThrow());
        spark.setAppResource("file://" + appJar);

        spark.setMainClass(runContext.render(mainClass).as(String.class).orElseThrow());

        runContext.render(jars).asMap(String.class, String.class)
            .forEach(throwBiConsumer((key, value) -> spark.addJar(this.tempFile(runContext, key, value))));
    }
}
