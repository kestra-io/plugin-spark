package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.serializers.JacksonMapper;

/**
 * In-process stand-in for the Spark standalone Master REST API, replaying the payloads of Spark 4.0
 * ({@code org.apache.spark.deploy.rest.StandaloneRestServer}).
 */
public class StubSparkMaster implements AutoCloseable {
    public static final String SUBMISSION_ID = "driver-20261004120000-0000";

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    private final HttpServer server;
    private final ConcurrentLinkedDeque<String> states = new ConcurrentLinkedDeque<>();

    public final List<String> createBodies = new CopyOnWriteArrayList<>();
    public final List<String> statusRequests = new CopyOnWriteArrayList<>();
    public final List<String> killRequests = new CopyOnWriteArrayList<>();

    public volatile boolean rejectCreate = false;
    public volatile boolean driverFound = true;
    public volatile String driverMessage = null;

    public StubSparkMaster() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        this.server.createContext("/v1/submissions/create", this::create);
        this.server.createContext("/v1/submissions/status/", this::status);
        this.server.createContext("/v1/submissions/kill/", this::kill);
        this.server.createContext("/v1/submissions/broken/", exchange -> respond(exchange, 500, "<html>Internal error</html>"));
        this.server.start();
    }

    public String url() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    /**
     * States returned by successive status requests; the last one is repeated forever.
     */
    public StubSparkMaster states(String... states) {
        this.states.clear();
        this.states.addAll(List.of(states));
        return this;
    }

    private void create(HttpExchange exchange) throws IOException {
        createBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

        if (rejectCreate) {
            respond(
                exchange, 200, Map.of(
                    "action", "CreateSubmissionResponse",
                    "serverSparkVersion", "4.0.1",
                    "success", false,
                    "message", "Current state is not alive: STANDBY. Can only accept driver submissions in ALIVE state."
                )
            );
            return;
        }

        respond(
            exchange, 200, Map.of(
                "action", "CreateSubmissionResponse",
                "serverSparkVersion", "4.0.1",
                "submissionId", SUBMISSION_ID,
                "success", true,
                "message", "Driver successfully submitted as " + SUBMISSION_ID
            )
        );
    }

    private void status(HttpExchange exchange) throws IOException {
        String submissionId = lastPathSegment(exchange);
        statusRequests.add(submissionId);

        String state = states.size() > 1 ? states.pollFirst() : states.peekFirst();
        var body = new java.util.LinkedHashMap<String, Object>();
        body.put("action", "SubmissionStatusResponse");
        body.put("serverSparkVersion", "4.0.1");
        body.put("submissionId", submissionId);
        body.put("success", driverFound);
        if (driverFound) {
            body.put("driverState", state);
            body.put("workerId", "worker-20261004115900-172.18.0.3-35000");
            body.put("workerHostPort", "172.18.0.3:35000");
        }
        if (driverMessage != null) {
            body.put("message", driverMessage);
        }

        respond(exchange, 200, body);
    }

    private void kill(HttpExchange exchange) throws IOException {
        String submissionId = lastPathSegment(exchange);
        killRequests.add(submissionId);

        respond(
            exchange, 200, Map.of(
                "action", "KillSubmissionResponse",
                "serverSparkVersion", "4.0.1",
                "submissionId", submissionId,
                "success", true,
                "message", "Kill request for " + submissionId + " submitted"
            )
        );
    }

    private static String lastPathSegment(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static void respond(HttpExchange exchange, int code, Object body) throws IOException {
        byte[] bytes = (body instanceof String string ? string : MAPPER.writeValueAsString(body)).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json;charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
