package com.zen.poc.consumer.listener;

import static com.zen.poc.consumer.ConsumerApplication.TOPIC_NAME;

import com.zen.poc.model.Money;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConcurrentKafkaListener {

    private static final int SLEEP_MILLIS = 1000; // ~100 msg/sec - should balance producer (for 100 concurrency level)

    @KafkaListener(topics = TOPIC_NAME, groupId = "concurrent-consumer",
        containerFactory = "concurrentKafkaListenerContainerFactory")
    public void handle(ConsumerRecord<String, Money> event, Acknowledgment ack, Consumer<String, Money> consumer) {
        log.info("Handling kafka event (thread: {}) value: {} from partition: {}", Thread.currentThread().getId(),
            event.value(), event.partition());
        log.info("Consumer subscription: {}, assignment: {}", consumer.subscription(), consumer.assignment());
        processRecord();
        ack.acknowledge();

    }

    private void processRecord() {
        try {
            Thread.sleep(SLEEP_MILLIS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

}
