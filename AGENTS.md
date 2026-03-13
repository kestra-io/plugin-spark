# Kestra Spark Plugin

## What

Utilize Apache Spark for data processing within Kestra pipelines. Exposes 4 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Spark, allowing orchestration of Apache Spark-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
