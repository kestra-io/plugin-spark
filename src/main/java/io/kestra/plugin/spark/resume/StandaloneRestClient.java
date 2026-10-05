package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.apache.spark.launcher.SparkLauncher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.serializers.JacksonMapper;

/**
 * Client for the Spark standalone Master REST API (protocol {@code v1}), the one {@code spark-submit} uses in cluster mode.
 */
public class StandaloneRestClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final String PROTOCOL_VERSION = "v1";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    // Spark answers with small JSON messages, anything bigger comes from another service
    static final int MAX_BODY_BYTES = 1024 * 1024;
    static final int MAX_BODY_IN_MESSAGE = 500;

    private final URI submissionsUri;
    private final HttpClient httpClient;

    public StandaloneRestClient(String restUrl) {
        this.submissionsUri = URI.create(stripTrailingSlash(restUrl) + "/" + PROTOCOL_VERSION + "/submissions/");
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();
    }

    /**
     * Submits a driver in cluster mode and returns its submission id.
     */
    public String create(ClusterSubmission submission) throws IOException, InterruptedException {
        var body = new LinkedHashMap<String, Object>();
        body.put("action", "CreateSubmissionRequest");
        body.put("clientSparkVersion", clientSparkVersion());
        body.put("appResource", submission.appResource());
        body.put("mainClass", submission.mainClass());
        body.put("appArgs", submission.appArgs());
        body.put("sparkProperties", submission.sparkProperties());
        body.put("environmentVariables", submission.environmentVariables());

        var request = HttpRequest.newBuilder(submissionsUri.resolve("create"))
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json;charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
            .build();

        var response = send(request, "CreateSubmissionResponse");
        var submissionId = text(response, "submissionId");
        if (!response.path("success").asBoolean(false) || submissionId == null) {
            throw new SubmissionRejectedException(
                "The Spark Master rejected the submission: " + Optional.ofNullable(text(response, "message")).map(StandaloneRestClient::truncate).orElse("no message")
            );
        }

        return submissionId;
    }

    public DriverStatus status(String submissionId) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(submissionsUri.resolve("status/" + encode(submissionId)))
            .timeout(REQUEST_TIMEOUT)
            .GET()
            .build();

        var response = send(request, "SubmissionStatusResponse");
        var found = response.path("success").asBoolean(false);

        return new DriverStatus(
            found,
            found ? parseState(text(response, "driverState")) : null,
            text(response, "workerHostPort"),
            Optional.ofNullable(text(response, "message")).map(StandaloneRestClient::truncate).orElse(null)
        );
    }

    /**
     * Requests the Master to kill a driver, returning whether the Master accepted the request.
     */
    public boolean kill(String submissionId) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(submissionsUri.resolve("kill/" + encode(submissionId)))
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
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        var status = response.statusCode();

        byte[] bytes;
        try (InputStream body = response.body()) {
            bytes = body.readNBytes(MAX_BODY_BYTES + 1);
        }
        var body = new String(bytes, 0, Math.min(bytes.length, MAX_BODY_BYTES), StandardCharsets.UTF_8);

        // a proxy may have forwarded the request before failing, so the outcome stays unknown
        if (status == 502 || status == 504) {
            throw new IOException(unexpected(request, status, body));
        }
        if (bytes.length > MAX_BODY_BYTES) {
            throw new UnexpectedResponseException(unexpected(request, status, "response larger than " + MAX_BODY_BYTES + " bytes"));
        }

        JsonNode json;
        try {
            json = MAPPER.readTree(body);
        } catch (IOException e) {
            throw new UnexpectedResponseException(unexpected(request, status, body));
        }

        if (json == null || !expectedAction.equals(text(json, "action"))) {
            var message = json == null ? null : text(json, "message");
            throw new UnexpectedResponseException(unexpected(request, status, message != null ? message : body));
        }

        return json;
    }

    private static String unexpected(HttpRequest request, int status, String detail) {
        return "Unexpected response from the Spark Master REST API at " + request.uri() + " (HTTP " + status + "): " + truncate(detail);
    }

    static String truncate(String value) {
        return value.length() <= MAX_BODY_IN_MESSAGE ? value : value.substring(0, MAX_BODY_IN_MESSAGE) + "... (truncated)";
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
        var node = json.get(field);
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

    /**
     * A server answered with something that is not a valid REST API message, so it did not create a driver.
     */
    public static class UnexpectedResponseException extends IOException {
        public UnexpectedResponseException(String message) {
            super(message);
        }
    }
}
