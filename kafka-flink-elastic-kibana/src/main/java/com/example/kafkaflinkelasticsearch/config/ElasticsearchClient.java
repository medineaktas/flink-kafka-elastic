package com.example.kafkaflinkelasticsearch.config;

import org.apache.http.HttpHost;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
@Service
public class ElasticsearchClient {
    @Value("${elasticsearch.host}")
    private String host;
    @Value("${elasticsearch.port}")
    private int port;
    @Value("${elasticsearch.scheme}")
    private String scheme;

    public ElasticsearchClient() {
    }

    @Bean
    public RestClient getClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
        return builder.build();
    }

    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }
    public String getScheme() {
        return scheme;
    }
}
