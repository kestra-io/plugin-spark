package io.kestra.plugin.spark.resume;

import java.util.List;
import java.util.Map;

// Body of a CreateSubmissionRequest; appResource must be visible to every node of the cluster
public record ClusterSubmission(
    String appResource,
    String mainClass,
    List<String> appArgs,
    Map<String, String> sparkProperties,
    Map<String, String> environmentVariables) {
}
