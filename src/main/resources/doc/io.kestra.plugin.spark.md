# How to use the Apache Spark plugin

Submit Spark jobs and run spark-submit commands from Kestra flows, with dedicated tasks for each supported application type.

## Common properties

Spark cluster connection details — `master`, deploy mode, and any cluster-manager-specific settings — are set directly on each task rather than via a shared connection object. Credentials for underlying storage or cluster resources (S3 keys, Kerberos config, etc.) are passed through the `configurations` map as standard Spark configuration properties. For cloud-managed clusters (Dataproc, EMR, Databricks), use the dedicated plugin for that platform instead — this plugin targets standalone Spark clusters and local mode.

## Tasks

Choose the submit task that matches your application: `JarSubmit` for JVM applications, `PythonSubmit` for PySpark scripts, and `RSubmit` for SparkR scripts. `JarSubmit` requires `mainResource` (a JAR from Kestra internal storage) and `mainClass` (the fully qualified entry point class). `PythonSubmit` and `RSubmit` both take `mainScript` as inline script content. All three share `master`, `args`, and `configurations` for arbitrary Spark config key-value pairs.

`SparkCLI` runs raw `spark-submit` commands inside a container and is the right choice when you need flags or options not exposed by the typed submit tasks, or when migrating existing shell-based Spark submission scripts into Kestra flows.
