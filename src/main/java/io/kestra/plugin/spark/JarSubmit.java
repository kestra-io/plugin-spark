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
                containerImage: bitnami/spark
                taskRunner:
                    type: io.kestra.plugin.scripts.runner.docker.Docker
                    networkMode: host
                    user: root
                master: spark://localhost:7077
                mainResource: {{ inputs.file }}
                mainClass: spark.samples.App"""
        )
    }
)
public class JarSubmit extends AbstractSubmit {
    @Schema(
        title = "The main application resource.",
        description = "This should be the location of a JAR file for Scala/Java applications, or a Python script for PySpark applications.\n" +
            "Must be an internal storage URI."
    )
    @NotNull
    private Property<String> mainResource;

    @Schema(
        title = "The application class name for Java/Scala applications."
    )
    @NotNull
    private Property<String> mainClass;

    @Schema(
        title = "Additional JAR files to be submitted with the application.",
        description = "Must be an internal storage URI."
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
