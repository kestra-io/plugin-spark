package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandaloneRestClientTest {
    @Test
    void createSendsTheSparkSubmitProtocolMessage() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url() + "/")) {
            String submissionId = client.create(
                new ClusterSubmission(
                    "file:///opt/app.jar",
                    "com.example.App",
                    List.of("a", "b"),
                    Map.of("spark.app.name", "app"),
                    Map.of("SPARK_CONF", "x")
                )
            );

            assertThat(submissionId, is(StubSparkMaster.SUBMISSION_ID));
            assertThat(master.createBodies, hasSize(1));

            JsonNode body = JacksonMapper.ofJson().readTree(master.createBodies.getFirst());
            assertThat(body.get("action").asText(), is("CreateSubmissionRequest"));
            assertThat(body.get("clientSparkVersion").isNull(), is(false));
            assertThat(body.get("appResource").asText(), is("file:///opt/app.jar"));
            assertThat(body.get("mainClass").asText(), is("com.example.App"));
            assertThat(body.get("appArgs").get(1).asText(), is("b"));
            assertThat(body.get("sparkProperties").get("spark.app.name").asText(), is("app"));
            assertThat(body.get("environmentVariables").get("SPARK_CONF").asText(), is("x"));
        }
    }

    @Test
    void createFailsWhenTheMasterRejectsTheSubmission() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url())) {
            master.rejectCreate = true;

            var exception = assertThrows(
                StandaloneRestClient.SubmissionRejectedException.class,
                () -> client.create(new ClusterSubmission("file:///opt/app.jar", "com.example.App", List.of(), Map.of(), Map.of()))
            );
            assertThat(exception.getMessage(), containsString("STANDBY"));
        }
    }

    @Test
    void statusParsesTheDriverState() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING"); var client = new StandaloneRestClient(master.url())) {
            DriverStatus status = client.status(StubSparkMaster.SUBMISSION_ID);

            assertThat(status.found(), is(true));
            assertThat(status.state(), is(DriverState.RUNNING));
            assertThat(status.workerHostPort(), is("172.18.0.3:35000"));
            assertThat(master.statusRequests, contains(StubSparkMaster.SUBMISSION_ID));
        }
    }

    @Test
    void statusMapsAnUnknownStateToUnknown() throws Exception {
        try (var master = new StubSparkMaster().states("SOME_FUTURE_STATE"); var client = new StandaloneRestClient(master.url())) {
            assertThat(client.status(StubSparkMaster.SUBMISSION_ID).state(), is(DriverState.UNKNOWN));
        }
    }

    @Test
    void statusReportsAnUnknownDriver() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url())) {
            master.driverFound = false;

            DriverStatus status = client.status(StubSparkMaster.SUBMISSION_ID);
            assertThat(status.found(), is(false));
            assertThat(status.state(), nullValue());
        }
    }

    @Test
    void killRequestsTheDriverKill() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url())) {
            assertThat(client.kill(StubSparkMaster.SUBMISSION_ID), is(true));
            assertThat(master.killRequests, contains(StubSparkMaster.SUBMISSION_ID));
        }
    }

    @Test
    void unexpectedPayloadIsAnIOException() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url() + "/v1/submissions/broken/x/y")) {
            // the base URL now points to a path answering HTML instead of a REST protocol message
            assertThrows(IOException.class, () -> client.status(StubSparkMaster.SUBMISSION_ID));
        }
    }
}
