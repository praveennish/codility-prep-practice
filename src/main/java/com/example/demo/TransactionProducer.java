package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionProducer{
    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, TransactionEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(TransactionEvent event){
        log.info("Producing transaction: {}", event.transactionId());
        /**
         * The second argument is the Message Key. Kafka runs the key through a hashing algorithm (MurmurHash3) to determine which of our 3 partitions the message lands on.
         * Passing a specific identifier ensures that every single event belonging to that specific transaction ID always lands on the exact same partition, guaranteeing 
         * chronological order of operations for that specific record.
         */
        this.kafkaTemplate.send(KafkaConfig.MAIN_TOPIC, event.transactionId(), event);
    }
}