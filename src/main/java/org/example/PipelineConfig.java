package org.example;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipelineConfig {
    private List<Source> sources;
    private List<Process> process;
    private List<Sink> sink;

    // Getters et setters

    public List<Source> getSources() {
        return sources;
    }

    public void setSources(List<Source> sources) {
        this.sources = sources;
    }

    public List<Process> getProcess() {
        return process;
    }

    public void setProcess(List<Process> process) {
        this.process = process;
    }

    public List<Sink> getSink() {
        return sink;
    }

    public void setSink(List<Sink> sink) {
        this.sink = sink;
    }

    public static class Source {
        private String key;
        private String path;
        private String format;
        private Map<String, String> options;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        public Map<String, String> getOptions() {
            return options != null ? options : Collections.emptyMap();
        }
    }

    public static class Process {
        private String key;
        private String query;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }

    public static class Sink {
        private String input;
        private String path;
        private String format;
        private Map<String, String> options;
        private List<String> partitionKey;
        private String mode;

        public String getInput() {
            return input;
        }

        public void setInput(String input) {
            this.input = input;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        public Map<String, String> getOptions() {
            return options != null ? options : Collections.emptyMap();
        }
        public void setPartitionKey(List<String> partitionKey) {
            this.partitionKey = partitionKey;
        }

        public List<String> getPartitionKey() {
            return partitionKey != null ? partitionKey : Collections.emptyList();
        }

        public String getPartitionSafeKey() {
            if (partitionKey == null || partitionKey.isEmpty()) {
                return "";
            }
            return String.join(",", partitionKey);
        }
        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }
    }
}
