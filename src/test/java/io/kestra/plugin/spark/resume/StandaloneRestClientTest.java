package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandaloneRestClientTest {
    private static final ClusterSubmission SUBMISSION = new ClusterSubmission(
        "file:///opt/app.jar", "com.example.App", List.of(), Map.of(), Map.of()
    );

    @Test
    void createSendsTheSparkSubmitProtocolMessage() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url() + "/")) {
            var submissionId = client.create(
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

            var body = JacksonMapper.ofJson().readTree(master.createBodies.getFirst());
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

            var exception = assertThrows(StandaloneRestClient.SubmissionRejectedException.class, () -> client.create(SUBMISSION));
            assertThat(exception.getMessage(), containsString("STANDBY"));
        }
    }

    @Test
    void sparkErrorResponseIsAnUnexpectedResponse() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url())) {
            master.errorOnCreate = true;

            var exception = assertThrows(StandaloneRestClient.UnexpectedResponseException.class, () -> client.create(SUBMISSION));
            assertThat(exception.getMessage(), containsString("HTTP 400"));
            assertThat(exception.getMessage(), containsString("Malformed request"));
        }
    }

    @Test
    void gatewayTimeoutIsNotAnUnexpectedResponse() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url())) {
            master.gatewayTimeoutOnCreate = true;

            // a proxy may have forwarded the request, so whether a driver exists stays unknown
            var exception = assertThrows(IOException.class, () -> client.create(SUBMISSION));
            assertThat(exception, not(instanceOf(StandaloneRestClient.UnexpectedResponseException.class)));
            assertThat(exception.getMessage(), containsString("HTTP 504"));
        }
    }

    @Test
    void anotherServiceIsAnUnexpectedResponseWithATruncatedBody() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url() + "/long-html")) {
            var exception = assertThrows(StandaloneRestClient.UnexpectedResponseException.class, () -> client.status(StubSparkMaster.SUBMISSION_ID));

            assertThat(exception.getMessage(), containsString("HTTP 404"));
            assertThat(exception.getMessage(), containsString("(truncated)"));
            assertThat(exception.getMessage().length(), lessThan(StandaloneRestClient.MAX_BODY_IN_MESSAGE + 200));
        }
    }

    @Test
    void oversizedBodyIsAnUnexpectedResponse() throws Exception {
        try (var master = new StubSparkMaster(); var client = new StandaloneRestClient(master.url() + "/huge")) {
            var exception = assertThrows(StandaloneRestClient.UnexpectedResponseException.class, () -> client.status(StubSparkMaster.SUBMISSION_ID));

            assertThat(exception.getMessage(), containsString("larger than"));
        }
    }

    @Test
    void statusParsesTheDriverState() throws Exception {
        try (var master = new StubSparkMaster().states("RUNNING"); var client = new StandaloneRestClient(master.url())) {
            var status = client.status(StubSparkMaster.SUBMISSION_ID);

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

            var status = client.status(StubSparkMaster.SUBMISSION_ID);
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
}
