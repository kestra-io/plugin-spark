# How to use the Apache Spark plugin

Submit Spark jobs and run spark-submit commands from Kestra flows, with dedicated tasks for each supported application type.

## Common properties

Spark cluster connection details — `master`, deploy mode, and any cluster-manager-specific settings — are set directly on each task rather than via a shared connection object. Credentials for underlying storage or cluster resources (S3 keys, Kerberos config, etc.) are passed through the `configurations` map as standard Spark configuration properties. For cloud-managed clusters (Dataproc, EMR, Databricks), use the dedicated plugin for that platform instead — this plugin targets standalone Spark clusters and local mode.

## Tasks

Choose the submit task that matches your application: `JarSubmit` for JVM applications, `PythonSubmit` for PySpark scripts, and `RSubmit` for SparkR scripts. `JarSubmit` requires `mainResource` (a JAR from Kestra internal storage) and `mainClass` (the fully qualified entry point class). `PythonSubmit` and `RSubmit` both take `mainScript` as inline script content. All three share `master`, `args`, and `configurations` for arbitrary Spark config key-value pairs.

`SparkCLI` runs raw `spark-submit` commands inside a container and is the right choice when you need flags or options not exposed by the typed submit tasks, or when migrating existing shell-based Spark submission scripts into Kestra flows.

## Resuming after a worker restart

When a Kestra worker restarts while a task runs (deployment, crash), the task is resubmitted to another worker. By default, that runs `spark-submit` again, so the application runs twice: with append-mode writes such as `INSERT INTO`, data is written twice.

Set `resume: true` on `JarSubmit` to avoid that on a Spark standalone cluster:

```yaml
- id: jar_submit
  type: io.kestra.plugin.spark.JarSubmit
  master: spark://spark-master:7077
  deployMode: CLUSTER
  resume: true
  mainResource: file:///opt/spark/examples/jars/spark-examples.jar
  mainClass: org.apache.spark.examples.SparkPi
```

The task then submits the driver through the Spark Master REST API, stores its submission id in the KV store of the flow namespace, and polls the driver until it ends. The resubmitted attempt finds the submission id and re-attaches to the same driver instead of submitting the application again. The task also reflects the outcome of the driver: it fails when the driver fails, errors or is killed, which `spark-submit` does not report in standalone cluster mode.

Requirements:

- `deployMode: CLUSTER`, so that the driver runs on the cluster and survives the worker. In client mode the driver runs inside the task and stops with it.
- A standalone `spark://` master with the REST API enabled (`spark.master.rest.enabled=true`), reachable from the Kestra worker. `restUrl` defaults to port `6066` of the master host. The REST API is not authenticated unless you secure it, for example with `spark.master.rest.filters`.
- `JarSubmit`: Spark standalone does not support cluster deploy mode for Python and R applications.
- The application jar, `jars` and `appFiles` must be URIs visible to every node of the cluster (`hdfs://`, `https://`, `s3a://`, or `file://` for a path present on every node), not Kestra internal storage URIs, since the driver runs on a Spark worker.

Behavior:

- Killing the execution or reaching the task `timeout` kills the driver. A Kestra retry after the driver failed submits a new driver.
- If the outcome of a previous submission cannot be determined (the worker stopped while the submission request was in flight, or the Master no longer knows the driver), the task fails instead of submitting a possible duplicate. The error message names the KV entry to delete once you checked the Spark Master UI.
- The driver output stays in the Spark worker logs; the task logs the driver state transitions and returns `submissionId`, `driverState` and `resumed` in its `vars` output.
- Resuming prevents a duplicate submission, not duplicate writes made by the application itself. Keep writes idempotent: transactional table formats (Delta, Iceberg, Hudi), `MERGE` instead of `INSERT INTO`, or output paths scoped to `{{ execution.id }}`.
