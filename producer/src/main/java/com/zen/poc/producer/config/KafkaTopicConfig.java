package com.zen.poc.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic topic100() {
        return TopicBuilder.name("topic-100")
            .partitions(100)
            .replicas(1)
            .build();
    }
}
