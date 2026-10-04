package io.kestra.plugin.spark.resume;

import java.util.List;
import java.util.Map;

/**
 * Body of a {@code CreateSubmissionRequest} sent to the Spark standalone Master REST API.
 *
 * @param appResource the application jar, which must be visible to every node of the cluster
 * @param mainClass the application entry point
 * @param appArgs the application arguments
 * @param sparkProperties the Spark properties of the driver
 * @param environmentVariables the environment variables of the driver
 */
public record ClusterSubmission(
    String appResource,
    String mainClass,
    List<String> appArgs,
    Map<String, String> sparkProperties,
    Map<String, String> environmentVariables) {
}
