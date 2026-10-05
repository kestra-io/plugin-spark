package io.kestra.plugin.spark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpConnectTimeoutException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.KestraSparkLauncher;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.kestra.plugin.spark.resume.ClusterSubmission;
import io.kestra.plugin.spark.resume.DriverState;
import io.kestra.plugin.spark.resume.DriverStatus;
import io.kestra.plugin.spark.resume.ResumeRecord;
import io.kestra.plugin.spark.resume.ResumeStateStore;
import io.kestra.plugin.spark.resume.StandaloneRestClient;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSubmit extends Task implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "apache/spark:4.0.1-java21-r";
    private static final String SPARK_MASTER_PREFIX = "spark://";
    private static final int DEFAULT_REST_PORT = 6066;
    // How long the Master REST API may stay unreachable while polling
    private static final Duration STATUS_ERROR_TOLERANCE = Duration.ofMinutes(5);
    private static final Duration MIN_POLL_INTERVAL = Duration.ofSeconds(1);

    @Schema(
        title = "Set Spark master endpoint",
        description = "Required Spark master URL (e.g., `spark://host:port`, `local[*]`). Must follow Spark master URL formats."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> master;

    @Schema(
        title = "Name the Spark application",
        description = "Optional application name passed to Spark; falls back to Spark defaults when empty."
    )
    @PluginProperty(group = "advanced")
    private Property<String> name;

    @Schema(
        title = "Pass arguments to application",
        description = "Command-line arguments forwarded to the application in order."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> args;

    @Schema(
        title = "Ship additional files with job",
        description = "Map of local filenames to internal storage URIs; each file is downloaded to the working directory and sent with --files."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> appFiles;

    @Schema(
        title = "Enable verbose spark-submit output",
        description = "Defaults to false; sets --verbose for detailed submission logs."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> verbose = Property.ofValue(false);

    @Schema(
        title = "Spark configuration overrides",
        description = "Key/value Spark configurations applied via --conf before submission."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> configurations;

    @Schema(
        title = "Choose Spark deploy mode",
        description = "client or cluster; defaults to client when unset."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<DeployMode> deployMode = Property.ofValue(DeployMode.CLIENT);

    @Schema(
        title = "Path to spark-submit binary",
        description = "Absolute path to spark-submit; defaults to `/opt/spark/bin/spark-submit`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> sparkSubmitPath = Property.ofValue("/opt/spark/bin/spark-submit");

    @Schema(
        title = "Environment variables for spark-submit",
        description = "Rendered key/value pairs added to the submission process environment."
    )
    @PluginProperty(group = "execution")
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Execution engine (deprecated)",
        description = "Deprecated; use taskRunner instead. Defaults to RunnerType.DOCKER when specified."
    )
    @PluginProperty(group = "execution")
    protected Property<RunnerType> runner;

    @Schema(
        title = "Docker runner options (deprecated)",
        description = "Deprecated in favor of taskRunner; only applied when using the legacy runner property."
    )
    @PluginProperty(group = "execution")
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner implementation",
        description = "Runner definition (e.g., Docker). Defaults to the Docker task runner; each runner exposes its own properties."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Container image for task runner",
        description = "Used when the task runner is container-based; defaults to `apache/spark:4.0.1-java21-r`."
    )
    @PluginProperty(dynamic = true, group = "execution")
    @Builder.Default
    private String containerImage = DEFAULT_IMAGE;

    @Schema(
        title = "Resume the Spark driver after a worker restart",
        description = """
            Defaults to false. When true, the driver is submitted through the Spark standalone Master REST API, its \
            submission id is stored in the KV store of the flow namespace, and the task polls the driver until it \
            ends: the task succeeds only if the driver finishes, and fails if the driver fails, errors or is killed. \
            If the worker restarts while the driver runs, the resubmitted attempt re-attaches to the same driver \
            instead of submitting the application a second time.

            Requires `deployMode: CLUSTER`, a standalone `spark://` master with `spark.master.rest.enabled=true`, and \
            the `JarSubmit` task (Spark standalone does not support cluster deploy mode for Python and R \
            applications). The application jar and any `jars` or `appFiles` must be URIs visible to every node of the \
            cluster (for example `hdfs://`, `https://`, `s3a://`, or `file://` for a path present on every node), \
            as the driver runs on a Spark worker. The task runner is not used in this mode, and the driver output \
            stays in the Spark worker logs.

            Killing the execution, or reaching the task `timeout`, kills the driver. Resuming prevents a duplicate \
            submission, not duplicate writes made by the application itself: keep writes idempotent.

            The submission, including `env` and `configurations`, is sent to `restUrl` as is: over plain `http://` \
            anyone on the network path can read secrets such as storage keys. Use an `https://` endpoint (for example \
            a TLS proxy in front of the Master) or secure the REST API with `spark.master.rest.filters`."""
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> resume = Property.ofValue(false);

    @Schema(
        title = "Spark Master REST API endpoint",
        description = """
            Used when `resume` is true. Defaults to `http://<master host>:6066`, derived from `master`. Set it \
            explicitly when the REST API listens on another port or host, or when `master` lists several masters. \
            The submission, including `env` and `configurations`, travels in clear text over `http://`: prefer an \
            `https://` endpoint when it carries secrets."""
    )
    @PluginProperty(group = "execution")
    private Property<String> restUrl;

    @Schema(
        title = "Interval between two driver status checks",
        description = "Used when `resume` is true. Minimum 1 second, defaults to 5 seconds."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollInterval = Property.ofValue(Duration.ofSeconds(5));

    // Runtime state shared with kill(), which runs on another thread
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicBoolean killed = new AtomicBoolean(false);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final AtomicReference<String> currentSubmissionId = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private volatile String currentRestUrl;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private transient volatile Logger runLogger;

    abstract protected void configure(RunContext runContext, SparkLauncher spark) throws Exception;

    // Only JVM applications can run in standalone cluster mode, so the other tasks keep this default
    protected ClusterSubmission clusterSubmission(RunContext runContext) throws Exception {
        throw new IllegalArgumentException(
            "`resume` is only supported by `JarSubmit`: Spark standalone clusters do not support the cluster deploy mode for Python and R applications."
        );
    }

    protected DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        if (runContext.render(this.resume).as(Boolean.class).orElse(false)) {
            return this.runResumable(runContext);
        }

        SparkLauncher spark = new KestraSparkLauncher(this.envs(runContext))
            .setMaster(runContext.render(master).as(String.class).orElseThrow())
            .setVerbose(runContext.render(verbose).as(Boolean.class).orElse(false));

        if (this.name != null) {
            spark.setAppName(runContext.render(this.name).as(String.class).orElseThrow());
        }

        if (this.configurations != null) {
            runContext.render(this.configurations).asMap(String.class, String.class)
                .forEach(throwBiConsumer(spark::setConf));
        }

        if (this.args != null) {
            runContext.render(this.args).asList(String.class)
                .forEach(throwConsumer(spark::addAppArgs));
        }

        if (this.appFiles != null) {
            runContext.render(this.appFiles).asMap(String.class, String.class)
                .forEach(throwBiConsumer((key, val) -> spark.addFile(this.tempFile(runContext, key, val))));
        }

        if (this.deployMode != null) {
            spark.setDeployMode(runContext.render(this.deployMode).as(DeployMode.class).orElse(DeployMode.CLIENT).value());
        }

        runContext.logger().info(runContext.render(this.deployMode).as(DeployMode.class).orElse(DeployMode.CLIENT).value());

        this.configure(runContext, spark);

        List<String> commandsArgs = new ArrayList<>();
        commandsArgs.add(runContext.render(this.sparkSubmitPath).as(String.class).orElse("spark-submit"));
        commandsArgs.addAll(((KestraSparkLauncher) spark).getCommands());

        return new CommandsWrapper(runContext)
            .withEnv(this.envs(runContext))
            .withRunnerType(runContext.render(this.runner).as(RunnerType.class).orElse(RunnerType.DOCKER))
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(this.containerImage)
            .withCommands(Property.ofValue(commandsArgs))
            .run();
    }

    // The resume record is only deleted once the driver outcome is known, so a driver is never submitted twice blindly
    private ScriptOutput runResumable(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        this.runLogger = logger;

        var rMaster = runContext.render(this.master).as(String.class).filter(value -> !value.isBlank()).orElseThrow(() -> renderedEmpty("master"));
        var rDeployMode = runContext.render(this.deployMode).as(DeployMode.class).orElse(DeployMode.CLIENT);
        if (rDeployMode != DeployMode.CLUSTER) {
            throw new IllegalArgumentException(
                "`resume` requires `deployMode: CLUSTER`: in client mode the driver runs inside the task and stops with it, so there is nothing to resume."
            );
        }
        if (!rMaster.startsWith(SPARK_MASTER_PREFIX)) {
            throw new IllegalArgumentException(
                "`resume` is only supported on Spark standalone masters (`spark://host:port`), got '" + rMaster + "'."
            );
        }

        var rPollInterval = runContext.render(this.pollInterval).as(Duration.class).orElse(Duration.ofSeconds(5));
        if (rPollInterval.compareTo(MIN_POLL_INTERVAL) < 0) {
            throw new IllegalArgumentException("`pollInterval` must be at least " + MIN_POLL_INTERVAL + ", got " + rPollInterval + ".");
        }

        var rRestUrl = runContext.render(this.restUrl).as(String.class).orElseGet(() -> defaultRestUrl(rMaster));
        // validated before any state is written
        var submission = this.clusterSubmission(runContext);

        var store = new ResumeStateStore(runContext);
        var previous = store.get();
        var namespace = runContext.flowInfo().namespace();
        var resumed = previous.isPresent();

        String submissionId;
        String driverRestUrl;
        if (previous.isPresent()) {
            var record = previous.get();
            if (record.status() == ResumeRecord.Status.PENDING) {
                throw new IllegalStateException(
                    "A previous attempt of this task run was interrupted while submitting the driver to " + record.restUrl() +
                        ", so it cannot be determined whether a driver was created. The task fails instead of risking a duplicate submission: " +
                        "check the Spark Master UI, then delete the KV entry '" + store.key() + "' of namespace '" + namespace + "' to allow a new submission."
                );
            }

            submissionId = record.submissionId();
            // the driver lives on the Master it was submitted to
            driverRestUrl = record.restUrl();
            logger.info("Re-attaching to Spark driver '{}' submitted to {} by a previous attempt of this task run", submissionId, driverRestUrl);
        } else {
            driverRestUrl = rRestUrl;
            var rConfigurations = runContext.render(this.configurations).asMap(String.class, String.class);
            if (driverRestUrl.startsWith("http://") && (!submission.environmentVariables().isEmpty() || !rConfigurations.isEmpty())) {
                logger.warn(
                    "`env` and `configurations` are sent in clear text to {}: use an https:// endpoint or secure the REST API if they contain secrets",
                    driverRestUrl
                );
            }

            store.put(ResumeRecord.pending(driverRestUrl));
            submissionId = this.submit(store, driverRestUrl, submission);
            store.put(ResumeRecord.submitted(submissionId, driverRestUrl));
            logger.info("Submitted Spark driver '{}' to {}", submissionId, driverRestUrl);
        }

        this.currentRestUrl = driverRestUrl;
        this.currentSubmissionId.set(submissionId);
        if (this.killed.get()) {
            // kill() ran before the submission id was known
            this.killDriver(driverRestUrl, submissionId);
        }

        DriverStatus status;
        try (var client = new StandaloneRestClient(driverRestUrl)) {
            status = this.waitForDriver(logger, client, submissionId, rPollInterval, store, namespace);
        } catch (InterruptedException e) {
            if (this.killed.get()) {
                store.delete();
            } else {
                logger.warn(
                    "Task interrupted while Spark driver '{}' is still running, it is left running so that the resubmitted attempt re-attaches to it",
                    submissionId
                );
            }
            throw e;
        }

        store.delete();

        if (!status.state().isSuccess()) {
            throw new IllegalStateException(
                "Spark driver '" + submissionId + "' ended in state " + status.state() +
                    Optional.ofNullable(status.message()).map(message -> ": " + message).orElse(".")
            );
        }

        return ScriptOutput.builder()
            .exitCode(0)
            .vars(
                Map.of(
                    "submissionId", submissionId,
                    "driverState", status.state().name(),
                    "resumed", resumed
                )
            )
            .build();
    }

    private String submit(ResumeStateStore store, String driverRestUrl, ClusterSubmission submission) throws Exception {
        try (var client = new StandaloneRestClient(driverRestUrl)) {
            return client.create(submission);
        } catch (
            StandaloneRestClient.SubmissionRejectedException | StandaloneRestClient.UnexpectedResponseException | ConnectException
            | HttpConnectTimeoutException e
        ) {
            // the request never reached a Master, or was answered without creating a driver
            store.delete();
            throw e;
        } catch (InterruptedException e) {
            // a kill ends the task run, while a worker shutdown keeps the record for the resubmitted attempt
            if (this.killed.get()) {
                store.delete();
            }
            throw e;
        }
    }

    private DriverStatus waitForDriver(Logger logger, StandaloneRestClient client, String submissionId, Duration pollInterval, ResumeStateStore store,
        String namespace) throws IOException, InterruptedException {
        DriverState lastState = null;
        Instant unreachableSince = null;

        while (true) {
            DriverStatus status;
            try {
                status = client.status(submissionId);
                unreachableSince = null;
            } catch (IOException e) {
                var now = Instant.now();
                if (unreachableSince == null) {
                    unreachableSince = now;
                }
                if (Duration.between(unreachableSince, now).compareTo(STATUS_ERROR_TOLERANCE) > 0) {
                    throw new IOException(
                        "Unable to get the status of Spark driver '" + submissionId + "' for more than " + STATUS_ERROR_TOLERANCE +
                            ". The driver handle is kept, so a retry of this task run re-attaches to the driver instead of submitting it again.",
                        e
                    );
                }

                logger.warn("Unable to get the status of Spark driver '{}', retrying: {}", submissionId, e.getMessage());
                Thread.sleep(pollInterval.toMillis());
                continue;
            }

            if (!status.found()) {
                throw new IllegalStateException(
                    "The Spark Master does not know the driver '" + submissionId + "' anymore (for example after a Master restart without recovery, " +
                        "or once `spark.deploy.retainedDrivers` completed drivers are exceeded), so its outcome cannot be determined. " +
                        "The task fails instead of risking a duplicate submission: delete the KV entry '" + store.key() + "' of namespace '" + namespace +
                        "' to allow a new submission."
                );
            }

            if (status.state() != lastState) {
                if (status.workerHostPort() != null) {
                    logger.info("Spark driver '{}' is {} on worker {}", submissionId, status.state(), status.workerHostPort());
                } else {
                    logger.info("Spark driver '{}' is {}", submissionId, status.state());
                }
                lastState = status.state();
            }

            if (status.state().isTerminal()) {
                return status;
            }

            Thread.sleep(pollInterval.toMillis());
        }
    }

    // Called on execution kill or task timeout, but not on worker shutdown, so the driver survives a restart
    @Override
    public void kill() {
        if (!this.killed.compareAndSet(false, true)) {
            return;
        }

        var submissionId = this.currentSubmissionId.get();
        var driverRestUrl = this.currentRestUrl;
        if (submissionId != null && driverRestUrl != null) {
            this.killDriver(driverRestUrl, submissionId);
        }
    }

    private void killDriver(String driverRestUrl, String submissionId) {
        var logger = this.runLogger;
        try (var client = new StandaloneRestClient(driverRestUrl)) {
            if (client.kill(submissionId)) {
                logger.info("Spark driver '{}' killed", submissionId);
            } else {
                logger.warn("The Spark Master refused to kill driver '{}'", submissionId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while killing Spark driver '{}'", submissionId);
        } catch (Exception e) {
            logger.warn("Unable to kill Spark driver '{}': {}", submissionId, e.getMessage());
        }
    }

    // spark.master is left to the Master unless set in configurations, so the driver uses the address the Master advertises
    protected ClusterSubmission buildClusterSubmission(RunContext runContext, String appResource, String mainClass, List<String> jars) throws Exception {
        requireClusterVisible(appResource, "mainResource");
        jars.forEach(jar -> requireClusterVisible(jar, "jars"));

        var sparkProperties = new LinkedHashMap<>(runContext.render(this.configurations).asMap(String.class, String.class));

        if (this.name != null) {
            sparkProperties.put("spark.app.name", runContext.render(this.name).as(String.class).orElseThrow(() -> renderedEmpty("name")));
        }
        sparkProperties.putIfAbsent("spark.app.name", mainClass);
        sparkProperties.put("spark.submit.deployMode", DeployMode.CLUSTER.value());

        var allJars = new ArrayList<String>();
        Optional.ofNullable(sparkProperties.get("spark.jars")).filter(value -> !value.isBlank()).ifPresent(allJars::add);
        allJars.addAll(jars);
        allJars.add(appResource);
        sparkProperties.put("spark.jars", String.join(",", allJars));

        var files = new ArrayList<>(runContext.render(this.appFiles).asMap(String.class, String.class).values());
        files.forEach(file -> requireClusterVisible(file, "appFiles"));
        if (!files.isEmpty()) {
            Optional.ofNullable(sparkProperties.get("spark.files")).filter(value -> !value.isBlank()).ifPresent(files::addFirst);
            sparkProperties.put("spark.files", String.join(",", files));
        }

        return new ClusterSubmission(
            appResource,
            mainClass,
            runContext.render(this.args).asList(String.class),
            sparkProperties,
            this.envs(runContext)
        );
    }

    protected static IllegalArgumentException renderedEmpty(String property) {
        return new IllegalArgumentException("`" + property + "` rendered to an empty value.");
    }

    private static void requireClusterVisible(String uri, String property) {
        if (uri.startsWith("kestra://")) {
            throw new IllegalArgumentException(
                "With `resume` enabled, `" + property + "` must be a URI visible to every node of the Spark cluster (for example `hdfs://`, `https://`, " +
                    "`s3a://`, or `file://` for a path present on every node), got '" + uri + "': in cluster mode the driver runs on a Spark worker, which cannot read Kestra internal storage."
            );
        }
    }

    private static String defaultRestUrl(String master) {
        var hostPort = master.substring(SPARK_MASTER_PREFIX.length());
        if (hostPort.contains(",")) {
            throw new IllegalArgumentException(
                "`master` lists several masters, set `restUrl` to the REST API endpoint of the active one, got '" + master + "'."
            );
        }

        var host = URI.create("http://" + hostPort).getHost();
        if (host == null) {
            throw new IllegalArgumentException("Unable to derive `restUrl` from `master` '" + master + "', set `restUrl` explicitly.");
        }

        return "http://" + host + ":" + DEFAULT_REST_PORT;
    }

    private Map<String, String> envs(RunContext runContext) throws IllegalVariableEvaluationException {
        HashMap<String, String> result = new HashMap<>();

        runContext.render(this.env).asMap(String.class, String.class)
            .forEach(throwBiConsumer((s, s2) ->
            {
                result.put(runContext.render(s), runContext.render(s2));
            }));

        return result;
    }

    protected String tempFile(RunContext runContext, String name, String url) throws IOException, URISyntaxException {
        File file = runContext.workingDir().resolve(Path.of(name)).toFile();

        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            URI from = new URI(url);
            IOUtils.copyLarge(runContext.storage().getFile(from), fileOutputStream);

            return file.getAbsoluteFile().toString();
        }
    }

    public enum DeployMode {
        CLIENT("client"),
        CLUSTER("cluster");

        private final String value;

        DeployMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        public String value() {
            return value;
        }
    }
}
