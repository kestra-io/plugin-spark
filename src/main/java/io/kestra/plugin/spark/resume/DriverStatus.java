package io.kestra.plugin.spark.resume;

// found is false when the Master does not know the submission, state is then null
public record DriverStatus(boolean found, DriverState state, String workerHostPort, String message) {
}
