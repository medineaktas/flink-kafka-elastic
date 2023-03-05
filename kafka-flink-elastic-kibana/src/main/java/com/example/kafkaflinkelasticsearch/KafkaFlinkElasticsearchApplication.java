package com.example.kafkaflinkelasticsearch;

import com.example.kafkaflinkelasticsearch.service.FlinkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class KafkaFlinkElasticsearchApplication {
	@Autowired
	private FlinkService flinkService;
	public static void main(String[] args) {
		SpringApplication.run(KafkaFlinkElasticsearchApplication.class, args);
	}

	@PostConstruct
	public void runFlink() {
		flinkService.run();
	}
}
