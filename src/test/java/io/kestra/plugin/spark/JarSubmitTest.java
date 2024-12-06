package io.kestra.plugin.spark;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
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
        Flux<LogEntry> receive = TestsUtils.receive(logQueue);

        URL resource = JarSubmitTest.class.getClassLoader().getResource("app.jar");

        URI put = storageInterface.put(
            null,
            null,
            new URI("/file/storage/app.jar"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        JarSubmit task = JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.of("spark://localhost:37077"))
            .runner(Property.of(RunnerType.DOCKER))
            .docker(DockerOptions.builder()
                .image("bitnami/spark:3.4.1")
                .entryPoint(List.of(""))
                .networkMode("host")
                .user("root")
                .build()
            )
            .mainClass(Property.of("spark.samples.App"))
            .mainResource(Property.of(put.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(receive.toStream().filter(logEntry -> logEntry.getMessage().contains("Lines with Lorem")).count(), is(1L));
    }
}
