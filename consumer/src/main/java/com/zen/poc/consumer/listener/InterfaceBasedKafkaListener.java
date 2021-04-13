package com.zen.poc.consumer.listener;

import com.zen.poc.model.Money;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class InterfaceBasedKafkaListener implements MessageListener<String, Money> {

    @Override
    public void onMessage(ConsumerRecord<String, Money> record) {
        System.out.println("Received: " + record.value());
    }
}
