package io.kestra.plugin.spark;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tasks.scripts.AbstractBash;
import io.kestra.core.tasks.scripts.ScriptOutput;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class JarSubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void jar() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        logQueue.receive(logs::add);

        URL resource = JarSubmitTest.class.getClassLoader().getResource("app.jar");

        URI put = storageInterface.put(
            new URI("/file/storage/app.jar"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        JarSubmit task = JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master("spark://localhost:37077")
            .runner(AbstractBash.Runner.DOCKER)
            .dockerOptions(AbstractBash.DockerOptions.builder()
                .image("bitnami/spark")
                .entryPoint(List.of("/bin/sh", "-c"))
                .networkMode("host")
                .user("root")
                .build()
            )
            .mainClass("spark.samples.App")
            .mainResource(put.toString())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage().contains("Lines with Lorem")).count(), is(1L));
    }
}
