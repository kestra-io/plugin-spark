package io.kestra.plugin.spark;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkLauncher;

import java.io.FileWriter;
import java.nio.file.Path;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit an R job to a remote cluster."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
            id: spark_r_submit
            namespace: company.team

            tasks:
                - id: r_submit
                type: io.kestra.plugin.spark.RSubmit
                containerImage: bitnami/spark
                taskRunner:
                    type: io.kestra.plugin.scripts.runner.docker.Docker
                    networkMode: host
                    user: root
                master: spark://localhost:7077
                mainScript: |
                    library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
                    sparkR.session()

                    print("The SparkR session has initialized successfully.")

                    sparkR.stop()"""
        )
    }
)
public class RSubmit extends AbstractSubmit {
    @Schema(
        title = "The main R script."
    )
    @NotNull
    private Property<String> mainScript;


    @Override
    protected void configure(RunContext runContext, SparkLauncher spark) throws Exception {
        Path path = runContext.workingDir().createTempFile(".R");
        try (FileWriter fileWriter = new FileWriter(path.toFile())) {
            IOUtils.write(runContext.render(this.mainScript).as(String.class).orElseThrow(), fileWriter);
            fileWriter.flush();
        }

        spark.setAppResource("file://" + path.toFile().getAbsolutePath());
    }
}
