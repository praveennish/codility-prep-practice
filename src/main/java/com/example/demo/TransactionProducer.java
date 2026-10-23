package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionProducer{
    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<Sring, TransactionEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(TransactionEvent event){
        log.info("Producing transaction: {}", event.transactionId());
        this.kafkaTemplate.send(KafkaConfig.MAIN_TOPIC, event.transactionId(), event);
    }
}