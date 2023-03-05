package com.example.kafkaflinkelasticsearch.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
@Component
public class FlinkService {
    @Autowired
    private FlinkKafkaConsumer011<String> consumer;

    @Autowired
    private ElasticsearchSinkService esSinkService;

    @Autowired
    private StreamExecutionEnvironment env;

    public void run() {
        DataStream<String> stream = env.addSource(consumer);

        ElasticsearchSink.Builder<String> esSinkBuilder = esSinkService.getEsSinkBuilder();

        stream.addSink(esSinkBuilder.build());

        try {
            env.setParallelism(1);
            env.execute("Flink Kafka Example");
        } catch (Exception e) {
            System.out.println("An error occurred while running the Flink job: " + e.getMessage());
        }
    }
}
