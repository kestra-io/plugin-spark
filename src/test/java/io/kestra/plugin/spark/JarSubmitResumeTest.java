package io.kestra.plugin.spark;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.spark.resume.ResumeRecord;
import io.kestra.plugin.spark.resume.ResumeStateStore;
import io.kestra.plugin.spark.resume.StandaloneRestClient;
import io.kestra.plugin.spark.resume.StubSparkMaster;

import jakarta.inject.Inject;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the resume decision table of {@link AbstractSubmit} against an in-process Spark Master REST API.
 */
@KestraTest
class JarSubmitResumeTest {
    private static final String APP_JAR = "file:///opt/spark/examples/jars/spark-examples.jar";
    private static final String MAIN_CLASS = "org.apache.spark.examples.SparkPi";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void submitsTheDriverAndWaitsForItsOutcome() throws Exception {
        try (var master = new StubSparkMaster().states("SUBMITTED", "RUNNING", "FINISHED")) {
            JarSubmit task = task(master)
                .name(Property.ofValue("pi"))
                .args(Property.ofValue(List.of("10")))
                .jars(Property.ofValue(Map.of("dep.jar", "hdfs:///libs/dep.jar")))
                .configurations(Property.ofValue(Map.of("spark.driver.memory", "512m")))
                .env(Property.ofValue(Map.of("SPARK_USER", "kestra")))
                .build();
            RunContext runContext = runContext(task);

            ScriptOutput output = task.run(runContext);

            assertThat(output.getExitCode(), is(0));
            assertThat(output.getVars().get("submissionId"), is(StubSparkMaster.SUBMISSION_ID));
            assertThat(output.getVars().get("driverState"), is("FINISHED"));
            assertThat(output.getVars().get("resumed"), is(false));
            assertThat(master.createBodies, hasSize(1));
            assertThat(store(runContext).get(), is(Optional.empty()));

            JsonNode body = JacksonMapper.ofJson().readTree(master.createBodies.getFirst());
            assertThat(body.get("appResource").asText(), is(APP_JAR));
            assertThat(body.get("mainClass").asText(), is(MAIN_CLASS));
            assertThat(body.get("appArgs").get(0).asText(), is("10"));
            assertThat(body.get("environmentVariables").get("SPARK_USER").asText(), is("kestra"));

            JsonNode sparkProperties = body.get("sparkProperties");
            assertThat(sparkProperties.get("spark.app.name").asText(), is("pi"));
            assertThat(sparkProperties.get("spark.submit.deployMode").asText(), is("cluster"));
            assertThat(sparkProperties.get("spark.driver.memory").asText(), is("512m"));
            assertThat(sparkProperties.get("spark.jars").asText(), is("hdfs:///libs/dep.jar," + APP_JAR));
            // left to the Master, which advertises the address the driver must use
            assertThat(sparkProperties.has("spark.master"), is(false));
        }
    }

    @Test
    void reattachesToTheDriverOfAPreviousAttemptInsteadOfSubmittingItAgain() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING", "FINISHED")) {
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);
            store(runContext).put(ResumeRecord.submitted(StubSparkMaster.SUBMISSION_ID, master.url()));

            ScriptOutput output = task.run(runContext);

            assertThat(output.getVars().get("resumed"), is(true));
            assertThat(output.getVars().get("driverState"), is("FINISHED"));
            assertThat(master.createBodies, empty());
            assertThat(master.statusRequests, everyItem(is(StubSparkMaster.SUBMISSION_ID)));
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void reattachesAfterTheWorkerInterruptsTheTaskOnShutdown() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            JarSubmit firstAttempt = task(master).build();
            RunContext runContext = runContext(firstAttempt);

            // the worker shutdown interrupts the task thread without calling kill()
            Thread worker = runInThread(firstAttempt, runContext, new AtomicReference<>());
            await().atMost(Duration.ofSeconds(10)).until(() -> isSubmitted(runContext));
            worker.interrupt();
            worker.join(10_000);

            assertThat(worker.isAlive(), is(false));
            assertThat(master.killRequests, empty());
            assertThat(store(runContext).get().map(ResumeRecord::submissionId), is(Optional.of(StubSparkMaster.SUBMISSION_ID)));

            // the resubmitted attempt of the same task run
            master.states("RUNNING", "FINISHED");
            ScriptOutput output = task(master).build().run(runContext);

            assertThat(output.getVars().get("resumed"), is(true));
            assertThat(output.getVars().get("submissionId"), is(StubSparkMaster.SUBMISSION_ID));
            assertThat(master.createBodies, hasSize(1));
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void killKillsTheDriverAndForgetsIt() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);
            AtomicReference<Throwable> error = new AtomicReference<>();

            Thread worker = runInThread(task, runContext, error);
            await().atMost(Duration.ofSeconds(10)).until(() -> isSubmitted(runContext));
            // the worker calls kill() then interrupts the task thread
            task.kill();
            worker.interrupt();
            worker.join(10_000);

            assertThat(master.killRequests, contains(StubSparkMaster.SUBMISSION_ID));
            assertThat(error.get(), instanceOf(InterruptedException.class));
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void failsWhenTheDriverFails() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING", "FAILED")) {
            master.driverMessage = "Exception from the cluster: java.lang.ClassNotFoundException";
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);

            var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext));

            assertThat(exception.getMessage(), containsString("FAILED"));
            assertThat(exception.getMessage(), containsString("ClassNotFoundException"));
            // the outcome is known, so a retry submits a new driver
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void refusesToSubmitAgainAfterAnInterruptedSubmission() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);
            store(runContext).put(ResumeRecord.pending(master.url()));

            var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext));

            assertThat(exception.getMessage(), containsString(store(runContext).key()));
            assertThat(master.createBodies, empty());
            assertThat(store(runContext).get().map(ResumeRecord::status), is(Optional.of(ResumeRecord.Status.PENDING)));
        }
    }

    @Test
    void refusesToSubmitAgainWhenTheMasterForgotTheDriver() throws Exception {
        try (var master = new StubSparkMaster()) {
            master.driverFound = false;
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);
            store(runContext).put(ResumeRecord.submitted(StubSparkMaster.SUBMISSION_ID, master.url()));

            var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext));

            assertThat(exception.getMessage(), containsString("does not know the driver"));
            assertThat(master.createBodies, empty());
            assertThat(store(runContext).get().isPresent(), is(true));
        }
    }

    @Test
    void forgetsARejectedSubmission() throws Exception {
        try (var master = new StubSparkMaster()) {
            master.rejectCreate = true;
            JarSubmit task = task(master).build();
            RunContext runContext = runContext(task);

            assertThrows(StandaloneRestClient.SubmissionRejectedException.class, () -> task.run(runContext));

            // no driver was created, so a retry can submit again
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void forgetsASubmissionThatCouldNotReachTheMaster() throws Exception {
        JarSubmit task = JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://localhost:7077"))
            .deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLUSTER))
            .resume(Property.ofValue(true))
            // nothing listens on this port
            .restUrl(Property.ofValue("http://localhost:1"))
            .mainResource(Property.ofValue(APP_JAR))
            .mainClass(Property.ofValue(MAIN_CLASS))
            .build();
        RunContext runContext = runContext(task);

        assertThrows(java.net.ConnectException.class, () -> task.run(runContext));

        assertThat(store(runContext).get(), is(Optional.empty()));
    }

    @Test
    void rejectsTheClientDeployMode() throws Exception {
        try (var master = new StubSparkMaster()) {
            JarSubmit task = task(master).deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLIENT)).build();

            assertRejected(task, "deployMode: CLUSTER");
            assertThat(master.createBodies, empty());
        }
    }

    @Test
    void rejectsNonStandaloneMasters() throws Exception {
        try (var master = new StubSparkMaster()) {
            assertRejected(task(master).master(Property.ofValue("k8s://https://kubernetes:443")).build(), "standalone");
            assertRejected(task(master).master(Property.ofValue("local[*]")).build(), "standalone");
        }
    }

    @Test
    void rejectsResourcesTheClusterCannotRead() throws Exception {
        try (var master = new StubSparkMaster()) {
            assertRejected(task(master).mainResource(Property.ofValue("kestra:///company/team/app.jar")).build(), "mainResource");
            assertRejected(task(master).jars(Property.ofValue(Map.of("dep.jar", "kestra:///company/team/dep.jar"))).build(), "jars");
            assertRejected(task(master).appFiles(Property.ofValue(Map.of("conf.json", "kestra:///company/team/conf.json"))).build(), "appFiles");
            assertThat(master.createBodies, empty());
        }
    }

    @Test
    void requiresRestUrlWithSeveralMasters() throws Exception {
        JarSubmit task = JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://master-1:7077,master-2:7077"))
            .deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLUSTER))
            .resume(Property.ofValue(true))
            .mainResource(Property.ofValue(APP_JAR))
            .mainClass(Property.ofValue(MAIN_CLASS))
            .build();

        assertRejected(task, "restUrl");
    }

    @Test
    void rejectsPythonApplications() throws Exception {
        try (var master = new StubSparkMaster()) {
            PythonSubmit task = PythonSubmit.builder()
                .id("unit-test")
                .type(PythonSubmit.class.getName())
                .master(Property.ofValue("spark://localhost:7077"))
                .deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLUSTER))
                .resume(Property.ofValue(true))
                .restUrl(Property.ofValue(master.url()))
                .mainScript(Property.ofValue("print('hello')"))
                .build();

            assertRejected(task, "JarSubmit");
            assertThat(master.createBodies, empty());
        }
    }

    private void assertRejected(AbstractSubmit task, String expectedMessage) throws Exception {
        RunContext runContext = runContext(task);

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString(expectedMessage));
        assertThat(store(runContext).get(), is(Optional.empty()));
    }

    private static JarSubmit.JarSubmitBuilder<?, ?> task(StubSparkMaster master) {
        return JarSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://localhost:7077"))
            .deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLUSTER))
            .resume(Property.ofValue(true))
            .restUrl(Property.ofValue(master.url()))
            .pollInterval(Property.ofValue(Duration.ofMillis(50)))
            .mainResource(Property.ofValue(APP_JAR))
            .mainClass(Property.ofValue(MAIN_CLASS));
    }

    private RunContext runContext(AbstractSubmit task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    private static ResumeStateStore store(RunContext runContext) {
        return new ResumeStateStore(runContext);
    }

    private static boolean isSubmitted(RunContext runContext) throws Exception {
        return store(runContext).get().map(record -> record.status() == ResumeRecord.Status.SUBMITTED).orElse(false);
    }

    private static Thread runInThread(AbstractSubmit task, RunContext runContext, AtomicReference<Throwable> error) {
        Thread thread = new Thread(() ->
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
