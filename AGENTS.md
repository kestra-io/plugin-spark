# Kestra Spark Plugin

## What

- Provides plugin components under `io.kestra.plugin.spark`.
- Includes classes such as `PythonSubmit`, `JarSubmit`, `RSubmit`, `SparkCLI`.

## Why

- What user problem does this solve? Teams need to submit Apache Spark jobs and run Spark CLI commands from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Spark steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Spark.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `spark`

Infrastructure dependencies (Docker Compose services):

- `spark-master`
- `spark-worker`

### Key Plugin Classes

- `io.kestra.plugin.spark.JarSubmit`
- `io.kestra.plugin.spark.PythonSubmit`
- `io.kestra.plugin.spark.RSubmit`
- `io.kestra.plugin.spark.SparkCLI`

### Project Structure

```
plugin-spark/
├── src/main/java/io/kestra/plugin/spark/
├── src/test/java/io/kestra/plugin/spark/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
