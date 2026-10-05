package io.kestra.plugin.spark.resume;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;

// The key ignores the attempt number, because an attempt resubmitted after a worker restart gets a new one
public class ResumeStateStore {
    // safety net for records left behind by abandoned executions
    static final Duration TTL = Duration.ofDays(7);

    private static final String KEY_PREFIX = "spark-resume_";

    private final KVStore kvStore;
    private final String key;

    public ResumeStateStore(RunContext runContext) {
        this.kvStore = runContext.namespaceKv(runContext.flowInfo().namespace());
        this.key = key(runContext);
    }

    public String key() {
        return key;
    }

    public Optional<ResumeRecord> get() throws IOException {
        try {
            return kvStore.getValue(key)
                .map(KVValue::value)
                .filter(Map.class::isInstance)
                .map(value -> ResumeRecord.fromMap((Map<?, ?>) value));
        } catch (ResourceExpiredException e) {
            return Optional.empty();
        }
    }

    public void put(ResumeRecord record) throws IOException {
        kvStore.put(
            key,
            new KVValueAndMetadata(new KVMetadata("Spark driver handle used to resume this task run after a worker restart", TTL), record.toMap())
        );
    }

    public void delete() throws IOException {
        kvStore.delete(key);
    }

    @SuppressWarnings("unchecked")
    static String key(RunContext runContext) {
        var variables = runContext.getVariables();
        var executionId = String.valueOf(((Map<String, Object>) variables.get("execution")).get("id"));
        var taskRunId = String.valueOf(((Map<String, Object>) variables.get("taskrun")).get("id"));

        return KEY_PREFIX + executionId + "_" + taskRunId;
    }
}
