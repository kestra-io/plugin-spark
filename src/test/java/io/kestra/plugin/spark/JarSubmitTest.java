package io.kestra.plugin.spark;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class JarSubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private DispatchQueueInterface<LogEntry> logQueue;

    @Test
    void jar() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        logQueue.addListener(logs::add);

        URL resource = JarSubmitTest.class.getClassLoader().getResource("app.jar");

        URI put = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/file/storage/app.jar"),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        JarSubmit task = JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://localhost:37077"))
            .taskRunner(
                Docker.builder()
                    .type(Docker.class.getName())
                    .entryPoint(List.of(""))
                    .networkMode("host")
                    .user("root")
                    .build()
            )
            .mainClass(Property.ofValue("spark.samples.App"))
            .mainResource(Property.ofValue(put.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        TestsUtils.awaitLogs(logs, logEntry -> logEntry.getMessage() != null && logEntry.getMessage().contains("Lines with Lorem"), 1);
        assertThat(logs.stream().filter(logEntry -> logEntry.getMessage() != null && logEntry.getMessage().contains("Lines with Lorem")).count(), is(1L));
    }
}
