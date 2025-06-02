package io.kestra.plugin.spark;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class PythonSubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void run() throws Exception {
        Flux<LogEntry> receive = TestsUtils.receive(logQueue);

        PythonSubmit task = PythonSubmit.builder()
            .id("unit-test")
            .type(JarSubmit.class.getName())
            .master(Property.ofValue("spark://localhost:37077"))
            .runner(Property.ofValue(RunnerType.DOCKER))
            .docker(DockerOptions.builder()
                .image("bitnami/spark:3.4.1")
                .entryPoint(List.of(""))
                .networkMode("host")
                .user("root")
                .build()
            )
            .name(Property.ofValue("PythonPiCalculate"))
            .args(Property.ofValue(List.of("10")))
            .mainScript(Property.ofValue("import sys\n" +
                "from random import random\n" +
                "from operator import add\n" +
                "\n" +
                "from pyspark.sql import SparkSession\n" +
                "\n" +
                "\n" +
                "if __name__ == \"__main__\":\n" +
                "    \"\"\"\n" +
                "        Usage: pi [partitions]\n" +
                "    \"\"\"\n" +
                "    spark = SparkSession \\\n" +
                "        .builder \\\n" +
                "        .appName(\"PythonPi\") \\\n" +
                "        .getOrCreate()\n" +
                "\n" +
                "    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2\n" +
                "    n = 100000 * partitions\n" +
                "\n" +
                "    def f(_: int) -> float:\n" +
                "        x = random() * 2 - 1\n" +
                "        y = random() * 2 - 1\n" +
                "        return 1 if x ** 2 + y ** 2 <= 1 else 0\n" +
                "\n" +
                "    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)\n" +
                "    print(\"Pi is roughly %f\" % (4.0 * count / n))\n" +
                "\n" +
                "    spark.stop()\n"
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        ScriptOutput runOutput = task.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(receive.toStream().filter(logEntry -> logEntry.getMessage().contains("Pi is roughly")).count(), is(1L));
    }
}
