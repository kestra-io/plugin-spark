package io.kestra.plugin.spark.resume;

/**
 * Status of a driver submission, as returned by {@code GET /v1/submissions/status/{submissionId}}.
 *
 * @param found whether the Master knows the submission ({@code success} in the REST response)
 * @param state the driver state, {@code null} when the submission is not found
 * @param workerHostPort the worker running the driver, if any
 * @param message an error message reported by the cluster, if any
 */
public record DriverStatus(boolean found, DriverState state, String workerHostPort, String message) {
}
