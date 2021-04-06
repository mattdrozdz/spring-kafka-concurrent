package com.zen.poc.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ConsumerApplication {

	public static final String TOPIC_NAME = "topic-100";

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}
}
