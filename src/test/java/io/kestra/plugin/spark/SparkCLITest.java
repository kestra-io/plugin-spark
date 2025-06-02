package io.kestra.plugin.spark;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SparkCLITest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void test() throws Exception {
        var spark = SparkCLI.builder()
            .id("spark-cli")
            .type(SparkCLI.class.getName())
            .commands(Property.ofValue(List.of("spark-submit --version")))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, spark, ImmutableMap.of());

        var output = spark.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getExitCode(), is(0));
    }

}