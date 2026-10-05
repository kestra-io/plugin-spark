package io.kestra.plugin.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.launcher.SparkLauncher;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.spark.resume.ClusterSubmission;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
        ),
        @Example(
            title = "Submit a job in cluster mode, and re-attach to the running driver instead of submitting it again if the Kestra worker restarts.",
            full = true,
            code = """
                id: spark_jar_submit_resume
                namespace: company.team

                tasks:
                  - id: jar_submit
                    type: io.kestra.plugin.spark.JarSubmit
                    master: spark://spark-master:7077
                    deployMode: CLUSTER
                    resume: true
                    restUrl: http://spark-master:6066
                    mainResource: file:///opt/spark/examples/jars/spark-examples.jar
                    mainClass: org.apache.spark.examples.SparkPi
                    args:
                      - "100\""""
        )
    }
)
public class JarSubmit extends AbstractSubmit {
    @Schema(
        title = "Application JAR resource",
        description = """
            Internal storage URI to the runnable application JAR uploaded to the working directory. With `resume` \
            enabled, a URI visible to every node of the Spark cluster instead (for example `hdfs://`, `https://`, \
            `s3a://`, or `file://` for a path present on every node), passed to the cluster as is."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> mainResource;

    @Schema(
        title = "Application main class",
        description = "Fully qualified entrypoint class passed to spark-submit `--class`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> mainClass;

    @Schema(
        title = "Additional dependency JARs",
        description = "Map of filenames to internal storage URIs added via `--jars`. With `resume` enabled, the values must be URIs visible to every node of the Spark cluster."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> jars;

    @Override
    protected ClusterSubmission clusterSubmission(RunContext runContext) throws Exception {
        return this.buildClusterSubmission(
            runContext,
            runContext.render(this.mainResource).as(String.class).filter(value -> !value.isBlank()).orElseThrow(() -> renderedEmpty("mainResource")),
            runContext.render(this.mainClass).as(String.class).filter(value -> !value.isBlank()).orElseThrow(() -> renderedEmpty("mainClass")),
            List.copyOf(runContext.render(this.jars).asMap(String.class, String.class).values())
        );
    }

    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        String appJar = this.tempFile(runContext, "app.jar", runContext.render(this.mainResource).as(String.class).orElseThrow());
        spark.setAppResource("file://" + appJar);

        spark.setMainClass(runContext.render(mainClass).as(String.class).orElseThrow());

        runContext.render(jars).asMap(String.class, String.class)
            .forEach(throwBiConsumer((key, value) -> spark.addJar(this.tempFile(runContext, key, value))));
    }
}
