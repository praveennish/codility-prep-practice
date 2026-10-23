package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class TransactionConsumer{
    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    @KafkaListener(
        topics = KafkaConfig.MAIN_TOPIC,
        groupId = "fin-processor-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
        TransactionEvent event = record.value();
         log.info("Received transaction for processing: {}", event.transactionId());

        // Simulation: negative amounts trigger business violations and push to DLQ
        if (event.amount() < 0) {
            log.error("Business Rule Violation: Negative transaction amount [{}] for ID: {}", event.amount(), event.transactionId());
            throw new IllegalArgumentException("Invalid transaction amount");
        }

        log.info("Successfully processed transaction: {}", event.transactionId());
        
        // Manual offset commit only on complete success
        ack.acknowledge();
    }
}