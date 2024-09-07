package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "Submit a Spark job to a remote cluster using a JAR file."
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
                    runner: DOCKER
                    master: spark://localhost:7077
                    mainResource: {{ inputs.file }}
                    mainClass: spark.samples.App
                """
        )
    }
)
public class JarSubmit extends AbstractSubmit {
    @Schema(
        title = "The main application resource.",
        description = "This should be the location of a JAR file for Scala/Java applications, or a Python script for PySpark applications.\n" +
            "Must be an internal storage URI."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainResource;

    @Schema(
        title = "The application class name for Java/Scala applications."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainClass;

    @Schema(
        title = "Additional JAR files to be submitted with the application.",
        description = "Must be an internal storage URI."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> jars;

    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        String appJar = this.tempFile(runContext, "app.jar", this.mainResource);
        spark.setAppResource("file://" + appJar);

        spark.setMainClass(runContext.render(mainClass));

        if (this.jars != null) {
            this.jars.forEach(throwBiConsumer((key, value) -> spark.addJar(this.tempFile(runContext, key, value))));
        }
    }
}
