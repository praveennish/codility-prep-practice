package com.example.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {KafkaConfig.MAIN_TOPIC, KafkaConfig.DLQ_TOPIC})
@DirtiesContext
public class TransactionIntegrationTest {

    @Autowired
    private TransactionProducer producer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testInvalidTransaction_RoutesToDeadLetterQueue() {
        // Arrange: Listen directly to the DLQ topic using raw String processing to verify content landing
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-dlq-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<String, String> dlqConsumer = cf.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dlqConsumer, KafkaConfig.DLQ_TOPIC);

        // Act: Emit an invalid transaction event (negative balance)
        TransactionEvent badEvent = new TransactionEvent("TX-999", "ACC-123", -250.00, "USD");
        producer.sendTransaction(badEvent);

        // Assert: Read the outcome from the DLQ topic
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(dlqConsumer, KafkaConfig.DLQ_TOPIC, 15000);
        
        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.value()).contains("TX-999");
        assertThat(singleRecord.value()).contains("-250.0");
        
        dlqConsumer.close();
    }
}
