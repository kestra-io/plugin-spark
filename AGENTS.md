# Kestra Spark Plugin

## What

- Provides plugin components under `io.kestra.plugin.spark`.
- Includes classes such as `PythonSubmit`, `JarSubmit`, `RSubmit`, `SparkCLI`.

## Why

- This plugin integrates Kestra with Apache Spark.
- It provides tasks that submit Apache Spark jobs and run Spark CLI commands.

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
