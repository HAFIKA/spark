package org.example;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.LoaderOptions;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class Main {

    public static void main(String[] args) throws Exception {

        String content = readConfigFileAndApplyVariables(args);
        Constructor constructor = new Constructor(PipelineConfig.class, new LoaderOptions());
        Yaml yaml = new Yaml(constructor);
        PipelineConfig config = yaml.load(content);

        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLDirectFileRead")
                .master("local[*]")
                .getOrCreate();

        List<PipelineConfig.Source> sources = config.getSources();
        List<PipelineConfig.Process> processes = config.getProcess();
        List<PipelineConfig.Sink> sinks = config.getSink();

        for (PipelineConfig.Source source : sources) {
            Dataset<Row> df = spark.read()
                    .format(source.getFormat())
                    .options(source.getOptions()) // méthode définie dans Source
                    .load(source.getPath());
            df.createOrReplaceTempView(source.getKey());
        }
        for (PipelineConfig.Process process : processes) {
            Dataset<Row> result = spark.sql(process.getQuery());
            result.createOrReplaceTempView(process.getKey());
        }
        for (PipelineConfig.Sink sink : sinks) {

            Dataset<Row> df = spark.table(sink.getInput());
            DataFrameWriter<Row> writer = df.write()
                    .format(sink.getFormat())
                    .mode(sink.getMode())
                    .options(sink.getOptions());

            if (!sink.getPartitionSafeKey().isEmpty()) {
                writer = writer.partitionBy(sink.getPartitionSafeKey());
            }
            writer.save(sink.getPath());
        }
    }

    private static String readConfigFileAndApplyVariables( String[] args) throws Exception {
        String yamlPath = args[0];
        String content = new String(Files.readAllBytes(Paths.get(yamlPath)));
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            String[] parts = arg.split("=", 2);
            if (parts.length == 2) {
                String key = parts[0];
                String value = parts[1];
                String placeholder = "{$" + key + "}";
                content = content.replace(placeholder, value);
            } else {
                System.err.println("Argument ignoré (format invalide): " + arg);
            }
        }
        return content;
    }
}
