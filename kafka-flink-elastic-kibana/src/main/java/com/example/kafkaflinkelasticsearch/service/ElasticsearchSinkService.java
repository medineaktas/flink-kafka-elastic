package com.example.kafkaflinkelasticsearch.service;


import com.example.kafkaflinkelasticsearch.config.ElasticsearchClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.Serializable;

@Service
public class ElasticsearchSinkService implements Serializable {

    private static final long serialVersionUID = -5789802938095816348L;

    @Value("${elasticsearch.index}")
    String index;
    @Value("${elasticsearch.type}")
    String type;
    @Autowired
    private transient ElasticsearchClient esClient;

    public ElasticsearchSink.Builder<String> getEsSinkBuilder() {
        final RestClient client = esClient.getClient();
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(esClient.getHost(), esClient.getPort(), esClient.getScheme()));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    private static final long serialVersionUID = -1907946323983359305L;
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        return Requests.indexRequest()
                                .index(index)
                                .type(type)
                                .source(json);
                    }
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                });
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder;
    }
}
