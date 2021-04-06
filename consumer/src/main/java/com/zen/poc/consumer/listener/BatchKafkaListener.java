package com.zen.poc.consumer.listener;

import static com.zen.poc.consumer.ConsumerApplication.TOPIC_NAME;

import com.zen.poc.model.Money;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BatchKafkaListener {

    private static final int THREADS = 200; // concurrency level x records in batch
    private static final int SLEEP_MILLIS = 2000; // 10 (concurrency level) x 20 (records in poll) => should ~ balance producer

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

    @KafkaListener(topics = TOPIC_NAME, groupId = "batch-consumer",
        containerFactory = "batchKafkaListenerContainerFactory")
    public void handle(@Payload List<Money> list, Acknowledgment ack, Consumer<String, Money> consumer) {
        log.info("Handling {} kafka events (thread: {})", list.size(), Thread.currentThread().getId());
        log.info("Consumer subscription: {}, assignment: {}", consumer.subscription(), consumer.assignment());
        // TODO: handle errors
        List<CompletableFuture<?>> futures = list.stream()
            .map(it -> CompletableFuture.supplyAsync(() -> processRecord(it), executorService)
                .orTimeout(5000, TimeUnit.MILLISECONDS)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Processing errored", error);
                    }
                }))
            .collect(Collectors.toList());
        futures.forEach(CompletableFuture::join);
        ack.acknowledge();
    }

    private Void processRecord(Money money) {
        log.info("Processing {} ...", money);
        try {
            Thread.sleep(SLEEP_MILLIS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
        return null;
    }

}
