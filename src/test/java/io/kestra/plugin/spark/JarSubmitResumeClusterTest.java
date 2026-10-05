package io.kestra.plugin.spark;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.spark.resume.DriverState;
import io.kestra.plugin.spark.resume.ResumeRecord;
import io.kestra.plugin.spark.resume.ResumeStateStore;
import io.kestra.plugin.spark.resume.StandaloneRestClient;

import jakarta.inject.Inject;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Runs resumable submissions against the Spark standalone cluster of docker-compose-ci.yml
@KestraTest
class JarSubmitResumeClusterTest {
    private static final String REST_URL = "http://localhost:36066";
    // shipped in the apache/spark image, so visible to every node of the cluster
    private static final String EXAMPLES_JAR = "file:///opt/spark/examples/jars/spark-examples.jar";
    private static final Duration TIMEOUT = Duration.ofMinutes(3);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runsTheDriverToCompletion() throws Exception {
        var task = sparkPi(10);

        var output = task.run(runContext(task));

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getVars().get("driverState"), is("FINISHED"));
        assertThat(output.getVars().get("resumed"), is(false));
    }

    @Test
    void reattachesToTheRunningDriverAfterAWorkerShutdown() throws Exception {
        var firstAttempt = sparkPi(2000);
        var runContext = runContext(firstAttempt);

        // the worker shutdown interrupts the task thread without calling kill()
        var worker = runInThread(firstAttempt, runContext, new AtomicReference<>());
        var submissionId = await().atMost(TIMEOUT).until(() -> submittedId(runContext), Optional::isPresent).orElseThrow();
        worker.interrupt();
        worker.join(30_000);

        try (StandaloneRestClient client = new StandaloneRestClient(REST_URL)) {
            assertThat(client.status(submissionId).state().isTerminal(), is(false));
        }

        // the resubmitted attempt of the same task run
        var output = sparkPi(2000).run(runContext);

        assertThat(output.getVars().get("resumed"), is(true));
        assertThat(output.getVars().get("submissionId"), is(submissionId));
        assertThat(output.getVars().get("driverState"), is("FINISHED"));
        assertThat(new ResumeStateStore(runContext).get(), is(Optional.empty()));
    }

    @Test
    void failsWhenTheDriverFails() throws Exception {
        var task = base()
            .mainClass(Property.ofValue("org.apache.spark.examples.DoesNotExist"))
            .build();

        var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext(task)));

        assertThat(exception.getMessage(), anyOf(containsString("FAILED"), containsString("ERROR")));
    }

    @Test
    void killKillsTheDriver() throws Exception {
        var task = sparkPi(100_000);
        var runContext = runContext(task);

        var worker = runInThread(task, runContext, new AtomicReference<>());
        var submissionId = await().atMost(TIMEOUT).until(() -> submittedId(runContext), Optional::isPresent).orElseThrow();
        task.kill();
        worker.interrupt();
        worker.join(30_000);

        try (StandaloneRestClient client = new StandaloneRestClient(REST_URL)) {
            await().atMost(TIMEOUT).until(() -> client.status(submissionId).state() == DriverState.KILLED);
        }
        assertThat(new ResumeStateStore(runContext).get(), is(Optional.empty()));
    }

    private static JarSubmit sparkPi(int partitions) {
        return base().args(Property.ofValue(List.of(String.valueOf(partitions)))).build();
    }

    private static JarSubmit.JarSubmitBuilder<?, ?> base() {
        return JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://localhost:37077"))
            .deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLUSTER))
            .resume(Property.ofValue(true))
            .restUrl(Property.ofValue(REST_URL))
            .pollInterval(Property.ofValue(Duration.ofSeconds(1)))
            .mainResource(Property.ofValue(EXAMPLES_JAR))
            .mainClass(Property.ofValue("org.apache.spark.examples.SparkPi"))
            .configurations(
                Property.ofValue(
                    Map.of(
                        // the address the driver uses from inside the Docker network
                        "spark.master", "spark://spark-master:7077",
                        "spark.driver.memory", "512m",
                        "spark.executor.memory", "512m",
                        "spark.cores.max", "1"
                    )
                )
            );
    }

    private RunContext runContext(JarSubmit task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    private static Optional<String> submittedId(RunContext runContext) throws Exception {
        return new ResumeStateStore(runContext).get()
            .filter(record -> record.status() == ResumeRecord.Status.SUBMITTED)
            .map(ResumeRecord::submissionId);
    }

    private static Thread runInThread(JarSubmit task, RunContext runContext, AtomicReference<Throwable> error) {
        var thread = new Thread(() ->
        {
            try {
                task.run(runContext);
            } catch (Throwable e) {
                error.set(e);
            }
        });
        thread.start();
        return thread;
    }
}
