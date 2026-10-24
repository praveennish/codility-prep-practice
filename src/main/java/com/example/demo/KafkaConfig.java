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
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig{
    @Autowired
    private org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;

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
        configMaps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers()); // Tells the client where to find the initial Kafka brokers to fetch cluster metadata.
        configMaps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // Converts our Java String message key into raw network bytes.
        configMaps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // Converts our complex Java object (TransactionEvent) into a raw JSON byte array using Jackson serialization.
        /**
         * It completely prevents data duplication caused by network glitches. If the producer sends a message, the broker writes it, but the network drops before 
         * the acknowledgment gets back to the producer, a standard producer would blindly re-send the message—causing a duplicate transaction. With idempotence enabled,
         * Kafka assigns a unique internal Sequence Number and Producer ID to every batch. If the broker receives a sequence number it has already written, 
         * it drops the duplicate and acknowledges it anyway.
         */
        configMaps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); 
        /**
         * Instructs the producer to wait until the absolute maximum level of acknowledgment is achieved. The "Leader" broker must receive the message and successfully sync 
         * it to all "In-Sync Replicas" (ISRs) 
         * before telling the producer the write was successful. This ensures zero data loss. 
         * */ 
        configMaps.put(ProducerConfig.ACKS_CONFIG, "all");
        // Allows up to 5 unacknowledged write requests to be sent concurrently over a single connection. When combined with ENABLE_IDEMPOTENCE_CONFIG, Kafka guarantees 
        // that even if batch #2 arrives before batch #1 due to a retry, the broker will rearrange and write them in their strict, original order.

        configMaps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Tells the producer to retry sending transient network failures indefinitely. Combined with idempotence, this is completely safe and guarantees the 
        // message will eventually land exactly once without creating duplicates.

        configMaps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        return new DefaultKafkaProducerFactory<>(configMaps);
    }

    @Bean
    public KafkaTemplate<String, TransactionEvent> resilientKafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, Object> consumerFactory
    ){
        ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // disable auto-commits entirely. The consumer app takes full responsibility for telling Kafka when a message is completed. 
        // If your database crashes mid-execution, the offset is never committed, and another consumer thread can safely grab the message and re-try it
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        /**
         * If a message contains un-parseable data or violates a business rule, a standard consumer loop will crash, pick up the exact same bad message, crash again, 
         * and get stuck forever (Head-of-Line Blocking). What it does: This establishes an automated circuit-breaker pattern:
         * FixedBackOff(1000L, 3) gives the message exactly 3 attempts, waiting 1 second (1000L) between each try to see if transient issues clear up.
         * If all 3 attempts fail, the DeadLetterPublishingRecoverer kicks in. It captures the poisonous message, attaches the root exception error message into the Kafka message headers, and publishes it out to a dedicated .DLQ error topic.
         * The error handler then immediately commits the offset of the bad record so the application can cleanly move on to the next healthy message in line.
         */
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(resilientKafkaTemplate());
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}