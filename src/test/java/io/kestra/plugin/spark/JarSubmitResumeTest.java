package io.kestra.plugin.spark;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.spark.resume.ResumeRecord;
import io.kestra.plugin.spark.resume.ResumeStateStore;
import io.kestra.plugin.spark.resume.StandaloneRestClient;
import io.kestra.plugin.spark.resume.StubSparkMaster;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Covers the resume decision table of AbstractSubmit against an in-process Spark Master REST API
@KestraTest
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class JarSubmitResumeTest {
    private static final String APP_JAR = "file:///opt/spark/examples/jars/spark-examples.jar";
    private static final String MAIN_CLASS = "org.apache.spark.examples.SparkPi";
    private static final Duration WAIT = Duration.ofSeconds(15);

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void submitsTheDriverAndWaitsForItsOutcome() throws Exception {
        try (var master = new StubSparkMaster().states("SUBMITTED", "RUNNING", "FINISHED")) {
            var task = task(master)
                .name(Property.ofValue("pi"))
                .args(Property.ofValue(List.of("10")))
                .jars(Property.ofValue(Map.of("dep.jar", "hdfs:///libs/dep.jar")))
                .configurations(Property.ofValue(Map.of("spark.driver.memory", "512m")))
                .env(Property.ofValue(Map.of("SPARK_USER", "kestra")))
                .build();
            var runContext = runContext(task);

            var output = task.run(runContext);

            assertThat(output.getExitCode(), is(0));
            assertThat(output.getVars().get("submissionId"), is(StubSparkMaster.SUBMISSION_ID));
            assertThat(output.getVars().get("driverState"), is("FINISHED"));
            assertThat(output.getVars().get("resumed"), is(false));
            assertThat(master.createBodies, hasSize(1));
            assertThat(store(runContext).get(), is(Optional.empty()));

            var body = JacksonMapper.ofJson().readTree(master.createBodies.getFirst());
            assertThat(body.get("appResource").asText(), is(APP_JAR));
            assertThat(body.get("mainClass").asText(), is(MAIN_CLASS));
            assertThat(body.get("appArgs").get(0).asText(), is("10"));
            assertThat(body.get("environmentVariables").get("SPARK_USER").asText(), is("kestra"));

            var sparkProperties = body.get("sparkProperties");
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
            var task = task(master).build();
            var runContext = runContext(task);
            store(runContext).put(ResumeRecord.submitted(StubSparkMaster.SUBMISSION_ID, master.url()));

            var output = task.run(runContext);

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
            var firstAttempt = task(master).build();
            var runContext = runContext(firstAttempt);

            // the worker shutdown interrupts the task thread without calling kill()
            var worker = runInThread(firstAttempt, runContext, new AtomicReference<>());
            await().atMost(WAIT).until(() -> hasRecord(runContext, ResumeRecord.Status.SUBMITTED));
            worker.interrupt();
            worker.join(WAIT.toMillis());

            assertThat(worker.isAlive(), is(false));
            assertThat(master.killRequests, empty());
            assertThat(store(runContext).get().map(ResumeRecord::submissionId), is(Optional.of(StubSparkMaster.SUBMISSION_ID)));

            // the resubmitted attempt of the same task run
            master.states("RUNNING", "FINISHED");
            var output = task(master).build().run(runContext);

            assertThat(output.getVars().get("resumed"), is(true));
            assertThat(output.getVars().get("submissionId"), is(StubSparkMaster.SUBMISSION_ID));
            assertThat(master.createBodies, hasSize(1));
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void killKillsTheDriverAndLogsItInTheExecution() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            var task = task(master).build();
            var runContext = runContext(task);
            var logs = logs();
            var error = new AtomicReference<Throwable>();

            var worker = runInThread(task, runContext, error);
            await().atMost(WAIT).until(() -> hasRecord(runContext, ResumeRecord.Status.SUBMITTED));
            // the worker calls kill() then interrupts the task thread
            task.kill();
            worker.interrupt();
            worker.join(WAIT.toMillis());

            assertThat(master.killRequests, contains(StubSparkMaster.SUBMISSION_ID));
            assertThat(error.get(), instanceOf(InterruptedException.class));
            assertThat(store(runContext).get(), is(Optional.empty()));
            await().atMost(WAIT).until(() -> containsMessage(logs, runContext, "Spark driver '" + StubSparkMaster.SUBMISSION_ID + "' killed"));
        }
    }

    @Test
    void killDuringTheSubmissionForgetsTheRecord() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            master.createDelayMillis = 10_000;
            var task = task(master).build();
            var runContext = runContext(task);
            var error = new AtomicReference<Throwable>();

            var worker = runInThread(task, runContext, error);
            await().atMost(WAIT).until(() -> !master.createBodies.isEmpty());
            task.kill();
            worker.interrupt();
            worker.join(WAIT.toMillis());

            assertThat(error.get(), instanceOf(InterruptedException.class));
            // a killed task run is not resubmitted, so the record must not block a later restart
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void shutdownDuringTheSubmissionKeepsThePendingRecord() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING")) {
            master.createDelayMillis = 10_000;
            var task = task(master).build();
            var runContext = runContext(task);

            var worker = runInThread(task, runContext, new AtomicReference<>());
            await().atMost(WAIT).until(() -> !master.createBodies.isEmpty());
            worker.interrupt();
            worker.join(WAIT.toMillis());

            // the request may have created a driver, so the resubmitted attempt must not submit blindly
            assertThat(store(runContext).get().map(ResumeRecord::status), is(Optional.of(ResumeRecord.Status.PENDING)));
        }
    }

    @Test
    void failsWhenTheDriverFails() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING", "FAILED")) {
            master.driverMessage = "Exception from the cluster: java.lang.ClassNotFoundException";
            var task = task(master).build();
            var runContext = runContext(task);

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
            var task = task(master).build();
            var runContext = runContext(task);
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
            var task = task(master).build();
            var runContext = runContext(task);
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
            var task = task(master).build();
            var runContext = runContext(task);

            assertThrows(StandaloneRestClient.SubmissionRejectedException.class, () -> task.run(runContext));

            // no driver was created, so a retry can submit again
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void forgetsASubmissionAnsweredWithASparkError() throws Exception {
        try (var master = new StubSparkMaster()) {
            master.errorOnCreate = true;
            var task = task(master).build();
            var runContext = runContext(task);

            assertThrows(StandaloneRestClient.UnexpectedResponseException.class, () -> task.run(runContext));

            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void forgetsASubmissionAnsweredByAnotherService() throws Exception {
        try (var master = new StubSparkMaster()) {
            // a wrong restUrl pointing to a service that answers with HTML
            var task = task(master).restUrl(Property.ofValue(master.url() + "/long-html")).build();
            var runContext = runContext(task);

            var exception = assertThrows(StandaloneRestClient.UnexpectedResponseException.class, () -> task.run(runContext));

            assertThat(exception.getMessage(), containsString("(truncated)"));
            assertThat(store(runContext).get(), is(Optional.empty()));
        }
    }

    @Test
    void keepsThePendingRecordWhenAProxyTimesOut() throws Exception {
        try (var master = new StubSparkMaster()) {
            master.gatewayTimeoutOnCreate = true;
            var task = task(master).build();
            var runContext = runContext(task);

            var exception = assertThrows(IOException.class, () -> task.run(runContext));

            assertThat(exception.getMessage(), containsString("HTTP 504"));
            assertThat(store(runContext).get().map(ResumeRecord::status), is(Optional.of(ResumeRecord.Status.PENDING)));
        }
    }

    @Test
    void forgetsASubmissionThatCouldNotReachTheMaster() throws Exception {
        var task = JarSubmit.builder()
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
        var runContext = runContext(task);

        assertThrows(java.net.ConnectException.class, () -> task.run(runContext));

        assertThat(store(runContext).get(), is(Optional.empty()));
    }

    @Test
    void warnsWhenSecretsTravelOverPlainHttp() throws Exception {
        try (var master = new StubSparkMaster().states("FINISHED")) {
            var task = task(master).env(Property.ofValue(Map.of("AWS_SECRET_ACCESS_KEY", "secret"))).build();
            var runContext = runContext(task);
            var logs = logs();

            task.run(runContext);

            await().atMost(WAIT).until(() -> containsMessage(logs, runContext, "sent in clear text"));
        }
    }

    @Test
    void doesNotWarnWithoutEnvOrConfigurations() throws Exception {
        try (var master = new StubSparkMaster().states("FINISHED")) {
            var task = task(master).build();
            var runContext = runContext(task);
            var logs = logs();

            task.run(runContext);

            await().atMost(WAIT).until(() -> containsMessage(logs, runContext, "is FINISHED"));
            assertThat(containsMessage(logs, runContext, "clear text"), is(false));
        }
    }

    @Test
    void rejectsPollIntervalsBelowOneSecond() throws Exception {
        try (var master = new StubSparkMaster()) {
            for (var interval : List.of(Duration.ZERO, Duration.ofMillis(500), Duration.ofSeconds(-1))) {
                assertRejected(task(master).pollInterval(Property.ofValue(interval)).build(), "pollInterval");
            }
            assertThat(master.createBodies, empty());
        }
    }

    @Test
    void rejectsTheClientDeployMode() throws Exception {
        try (var master = new StubSparkMaster()) {
            assertRejected(task(master).deployMode(Property.ofValue(AbstractSubmit.DeployMode.CLIENT)).build(), "deployMode: CLUSTER");
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
    void rejectsPropertiesRenderedToAnEmptyValue() throws Exception {
        try (var master = new StubSparkMaster()) {
            assertRejected(task(master).mainClass(Property.ofExpression("{{ null }}")).build(), "mainClass");
            assertRejected(task(master).mainResource(Property.ofExpression("{{ null }}")).build(), "mainResource");
            assertRejected(task(master).master(Property.ofValue(" ")).build(), "master");
            assertThat(master.createBodies, empty());
        }
    }

    @Test
    void requiresRestUrlWithSeveralMasters() throws Exception {
        var task = JarSubmit.builder()
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
            var task = PythonSubmit.builder()
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
        var runContext = runContext(task);

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
            .pollInterval(Property.ofValue(Duration.ofSeconds(1)))
            .mainResource(Property.ofValue(APP_JAR))
            .mainClass(Property.ofValue(MAIN_CLASS));
    }

    private RunContext runContext(AbstractSubmit task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    private List<LogEntry> logs() {
        var logs = new CopyOnWriteArrayList<LogEntry>();
        TestsUtils.receive(logQueue, either -> logs.add(either.getLeft()));
        return logs;
    }

    @SuppressWarnings("unchecked")
    private static boolean containsMessage(List<LogEntry> logs, RunContext runContext, String text) {
        var taskRunId = String.valueOf(((Map<String, Object>) runContext.getVariables().get("taskrun")).get("id"));
        return logs.stream()
            .filter(entry -> taskRunId.equals(entry.getTaskRunId()))
            .anyMatch(entry -> entry.getMessage() != null && entry.getMessage().contains(text));
    }

    private static ResumeStateStore store(RunContext runContext) {
        return new ResumeStateStore(runContext);
    }

    private static boolean hasRecord(RunContext runContext, ResumeRecord.Status status) throws Exception {
        return store(runContext).get().map(record -> record.status() == status).orElse(false);
    }

    private static Thread runInThread(AbstractSubmit task, RunContext runContext, AtomicReference<Throwable> error) {
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
