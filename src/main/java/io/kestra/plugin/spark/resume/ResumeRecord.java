package io.kestra.plugin.spark.resume;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handle of the driver submitted by a task run, so a resubmitted attempt can re-attach to it.
 *
 * @param status {@link Status#PENDING} while the submission request is in flight, {@link Status#SUBMITTED} once
 *        the Master returned a submission id
 * @param submissionId the driver submission id, {@code null} while {@link Status#PENDING}
 * @param restUrl the Master REST endpoint the driver was submitted to
 * @param updatedAt when the record was last written
 */
public record ResumeRecord(Status status, String submissionId, String restUrl, Instant updatedAt) {
    public enum Status {
        PENDING,
        SUBMITTED
    }

    public static ResumeRecord pending(String restUrl) {
        return new ResumeRecord(Status.PENDING, null, restUrl, Instant.now());
    }

    public static ResumeRecord submitted(String submissionId, String restUrl) {
        return new ResumeRecord(Status.SUBMITTED, submissionId, restUrl, Instant.now());
    }

    Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("status", status.name());
        map.put("submissionId", submissionId);
        map.put("restUrl", restUrl);
        map.put("updatedAt", updatedAt.toString());
        return map;
    }

    static ResumeRecord fromMap(Map<?, ?> map) {
        Object submissionId = map.get("submissionId");
        Object restUrl = map.get("restUrl");
        Object updatedAt = map.get("updatedAt");

        return new ResumeRecord(
            Status.valueOf(String.valueOf(map.get("status"))),
            submissionId == null ? null : submissionId.toString(),
            restUrl == null ? null : restUrl.toString(),
            updatedAt == null ? null : Instant.parse(updatedAt.toString())
        );
    }
}
