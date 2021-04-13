package com.zen.poc.consumer.config;

import com.zen.poc.consumer.listener.InterfaceBasedKafkaListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.LogIfLevelEnabled.Level;

@Configuration
public class InterfaceBasedListenerConfig {

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    interfaceBasedKafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(100); // max number of concurrent KafkaMessageListenerContainer running
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.getContainerProperties().setCommitLogLevel(Level.INFO);
        factory.getContainerProperties().setMessageListener(new InterfaceBasedKafkaListener());
        factory.getContainerProperties().setGroupId("interface-based-listener");
        return factory;
    }

    @Bean
    public MessageListenerContainer kafkaListenerContainer(
        KafkaListenerContainerFactory interfaceBasedKafkaListenerContainerFactory, InterfaceBasedKafkaListener interfaceBasedKafkaListener) {
        var container = interfaceBasedKafkaListenerContainerFactory.createContainer("topic-100");
        container.setupMessageListener(interfaceBasedKafkaListener);
        return container;
    }


}
