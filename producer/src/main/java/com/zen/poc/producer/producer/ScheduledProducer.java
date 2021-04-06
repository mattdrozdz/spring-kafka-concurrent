package com.zen.poc.producer.producer;

import static com.zen.poc.producer.ProducerApplication.TOPIC_NAME;

import com.zen.poc.model.Currency;
import com.zen.poc.model.Money;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class ScheduledProducer {

    private static final List<Currency> CURRENCIES = List.of(Currency.values());
    private static final int SIZE = CURRENCIES.size();
    private static final Random RANDOM = new Random();

    private final KafkaTemplate<String, Object> template;

    @Scheduled(fixedRate = 10) // ~100 msg/sec.
    public void produce() {
        BigDecimal value = BigDecimal.valueOf(Math.random() * 100).setScale(2, RoundingMode.HALF_UP);
        Currency currency = CURRENCIES.get(RANDOM.nextInt(SIZE));
        template.send(TOPIC_NAME, UUID.randomUUID().toString(),
            new Money(UUID.randomUUID(), value, currency));
    }

}
