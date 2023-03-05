package com.example.kafkaflinkelasticsearch.config;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class FlinkConfiguration {
    @Value("${kafka.topicName}")
    private String topicName;
    @Value("${kafka.port}")
    private int port;
    @Value("${kafka.host}")
    private String host;
    @Value("${kafka.groupId}")
    private String groupId;
    @Bean
    public FlinkKafkaConsumer011<String> kafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", host+":"+port);
        properties.setProperty("group.id", groupId);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topicName, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        return consumer;
    }

    @Bean
    public DataStream<String> dataStream(FlinkKafkaConsumer011<String> consumer, StreamExecutionEnvironment env) {
        DataStream<String> stream = env.addSource(consumer);
        return stream;
    }

    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, 1000));

        return env;
    }
}
