package org.example;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineConfigTest {

    @Test
    void testSourceDefaultsToEmptyOptions() {
        PipelineConfig.Source source = new PipelineConfig.Source();
        assertTrue(source.getOptions().isEmpty());

        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        source.setOptions(options);

        assertEquals("true", source.getOptions().get("header"));
    }

    @Test
    void testProcessSettersAndGetters() {
        PipelineConfig.Process process = new PipelineConfig.Process();
        process.setKey("step1");
        process.setQuery("SELECT * FROM table");

        assertEquals("step1", process.getKey());
        assertEquals("SELECT * FROM table", process.getQuery());
    }

    @Test
    void testSinkPartitionSafeKey() {
        PipelineConfig.Sink sink = new PipelineConfig.Sink();
        assertEquals("", sink.getPartitionSafeKey());

        sink.setPartitionKey(Arrays.asList("year", "month"));
        assertEquals("year,month", sink.getPartitionSafeKey());
    }

    @Test
    void testSinkDefaultsToEmptyOptionsAndPartitions() {
        PipelineConfig.Sink sink = new PipelineConfig.Sink();

        assertTrue(sink.getOptions().isEmpty());
        assertTrue(sink.getPartitionKey().isEmpty());

        Map<String, String> opts = new HashMap<>();
        opts.put("compression", "gzip");
        sink.setOptions(opts);

        assertEquals("gzip", sink.getOptions().get("compression"));
    }

    @Test
    void testPipelineConfigSetters() {
        PipelineConfig config = new PipelineConfig();

        PipelineConfig.Source source = new PipelineConfig.Source();
        source.setKey("src1");

        PipelineConfig.Process process = new PipelineConfig.Process();
        process.setKey("proc1");

        PipelineConfig.Sink sink = new PipelineConfig.Sink();
        sink.setInput("proc1");

        config.setSources(Collections.singletonList(source));
        config.setProcess(Collections.singletonList(process));
        config.setSink(Collections.singletonList(sink));

        assertEquals("src1", config.getSources().get(0).getKey());
        assertEquals("proc1", config.getProcess().get(0).getKey());
        assertEquals("proc1", config.getSink().get(0).getInput());
    }
}