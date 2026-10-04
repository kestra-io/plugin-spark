package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.launcher.SparkLauncher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;

/**
 * Client for the Spark standalone Master REST API (protocol {@code v1}), the one {@code spark-submit} uses in
 * standalone cluster mode. Requires {@code spark.master.rest.enabled=true} on the Master.
 */
public class StandaloneRestClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final String PROTOCOL_VERSION = "v1";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final URI submissionsUri;
    private final HttpClient httpClient;

    public StandaloneRestClient(String restUrl) {
        this.submissionsUri = URI.create(stripTrailingSlash(restUrl) + "/" + PROTOCOL_VERSION + "/submissions/");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();
    }

    /**
     * Submits a driver in cluster mode.
     *
     * @return the submission id assigned by the Master
     * @throws SubmissionRejectedException if the Master answered but refused the submission
     * @throws IOException if the Master could not be reached or answered with an unexpected payload
     */
    public String create(ClusterSubmission submission) throws IOException, InterruptedException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("action", "CreateSubmissionRequest");
        body.put("clientSparkVersion", clientSparkVersion());
        body.put("appResource", submission.appResource());
        body.put("mainClass", submission.mainClass());
        body.put("appArgs", submission.appArgs());
        body.put("sparkProperties", submission.sparkProperties());
        body.put("environmentVariables", submission.environmentVariables());

        HttpRequest request = HttpRequest.newBuilder(submissionsUri.resolve("create"))
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json;charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();

        JsonNode response = send(request, "CreateSubmissionResponse");
        String submissionId = text(response, "submissionId");
        if (!response.path("success").asBoolean(false) || submissionId == null) {
            throw new SubmissionRejectedException(
                "The Spark Master rejected the submission: " + Optional.ofNullable(text(response, "message")).orElse("no message")
            );
        }

        return submissionId;
    }

    /**
     * Fetches the current status of a driver.
     */
    public DriverStatus status(String submissionId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(submissionsUri.resolve("status/" + encode(submissionId)))
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .build();

        JsonNode response = send(request, "SubmissionStatusResponse");
        boolean found = response.path("success").asBoolean(false);
        String driverState = text(response, "driverState");

        return new DriverStatus(
            found,
            found ? parseState(driverState) : null,
            text(response, "workerHostPort"),
            text(response, "message")
        );
    }

    /**
     * Requests the Master to kill a driver.
     *
     * @return {@code true} if the Master accepted the kill request
     */
    public boolean kill(String submissionId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(submissionsUri.resolve("kill/" + encode(submissionId)))
            .timeout(REQUEST_TIMEOUT)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

        return send(request, "KillSubmissionResponse").path("success").asBoolean(false);
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private JsonNode send(HttpRequest request, String expectedAction) throws IOException, InterruptedException {
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        JsonNode json;
        try {
            json = MAPPER.readTree(response.body());
        } catch (IOException e) {
            throw new IOException(
                "Unexpected response from the Spark Master REST API at " + request.uri() + " (HTTP " + response.statusCode() + "): " + response.body(), e
            );
        }

        String action = text(json, "action");
        if (!expectedAction.equals(action)) {
            throw new IOException(
                "Unexpected response from the Spark Master REST API at " + request.uri() + " (HTTP " + response.statusCode() + "): " +
                    Optional.ofNullable(text(json, "message")).orElse(response.body())
            );
        }

        return json;
    }

    private static DriverState parseState(String driverState) {
        if (driverState == null) {
            return DriverState.UNKNOWN;
        }

        try {
            return DriverState.valueOf(driverState);
        } catch (IllegalArgumentException e) {
            // a state added by a future Spark version: keep polling
            return DriverState.UNKNOWN;
        }
    }

    private static String text(JsonNode json, String field) {
        JsonNode node = json.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String clientSparkVersion() {
        return Optional.ofNullable(SparkLauncher.class.getPackage().getImplementationVersion()).orElse("unknown");
    }

    /**
     * The Master answered but refused the submission, so no driver was created.
     */
    public static class SubmissionRejectedException extends IOException {
        public SubmissionRejectedException(String message) {
            super(message);
        }
    }
}
