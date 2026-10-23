package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig{
    public static final String MAIN_TOPIC = "financial-transactions";
    public static final String DLQ_TOPIC = "financial-transactions.DLQ";

    @Bean
    public NewTopic mainTopic(){
        return TopicBuilder.name(MAIN_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean 
    public NewTopic dlqTopic(){
        return TopicBuilder.name(DLQ_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public ProducerFactory<String,TransactionEvent> producerFactory()
    {
        Map<String, Object> configMaps = new HashMap<>();
        configMaps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMaps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMaps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configMaps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configMaps.put(ProducerConfig.ACKS_CONFIG, "all");
        configMaps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        configMaps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        return new DefaultKafkaProducerFactory<>(configMaps);
    }

    @Bean
    public KafkaTemplate<String, TransactionEvent> KafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, Object> consumerFactory,
        KafkaTemplate<String, TransactionEvent> template
    ){
        ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}