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
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit a jar spark job to remote cluster"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "runner: DOCKER",
                "master: spark://localhost:7077",
                "mainResource: {{ inputs.file }}",
                "mainClass: spark.samples.App",
            }
        )
    }
)
public class JarSubmit extends AbstractSubmit {
    @Schema(
        title = "the main application resource",
        description = "This should be the location of a jar file for Scala/Java applications, or a python script for PySpark applications.\n" +
            "Must be Kestra internal storage url"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainResource;

    @Schema(
        title = "the application class name for Java/Scala applications."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainClass;

    @Schema(
        title = "Adds jar files to be submitted with the application.",
        description = "Must be Kestra internal storage url"
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
