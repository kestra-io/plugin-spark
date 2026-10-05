package io.kestra.plugin.spark.resume;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

// PENDING while the create request is in flight, so an interrupted submission is never retried blindly
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
        var map = new LinkedHashMap<String, Object>();
        map.put("status", status.name());
        map.put("submissionId", submissionId);
        map.put("restUrl", restUrl);
        map.put("updatedAt", updatedAt.toString());
        return map;
    }

    static ResumeRecord fromMap(Map<?, ?> map) {
        var submissionId = map.get("submissionId");
        var restUrl = map.get("restUrl");
        var updatedAt = map.get("updatedAt");

        return new ResumeRecord(
            Status.valueOf(String.valueOf(map.get("status"))),
            submissionId == null ? null : submissionId.toString(),
            restUrl == null ? null : restUrl.toString(),
            updatedAt == null ? null : Instant.parse(updatedAt.toString())
        );
    }
}
