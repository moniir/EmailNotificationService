package com.example.monir.EmailNotificationService.config;

import com.example.monir.EmailNotificationService.error.NotRetryableException;
import com.example.monir.EmailNotificationService.error.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.client.HttpServerErrorException;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer Configuration with Error Handling
 * 
 * This Java-based configuration provides fine-grained control over:
 * - ErrorHandlingDeserializer wrapper for graceful deserialization error handling
 * - JacksonJsonDeserializer for Spring Boot 4/Spring Kafka 4 compatibility
 * - DefaultErrorHandler with custom retry logic and backoff strategy
 * - Dead Letter Topic (DLT) for publishing failed messages
 * 
 * ALTERNATIVE: Basic configuration can be achieved via application.properties
 * (see commented properties in application.properties for equivalent config)
 * However, Java configuration is recommended for:
 * - Complex error handling logic
 * - Custom retry strategies
 * - Dead Letter Topic (DLT) configuration
 * - Programmatic configuration based on environment/conditions
 */
@Configuration
public class KafkaConsumerConfiguration {

    @Autowired
    Environment environment;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        
        // Wrap deserializers with ErrorHandlingDeserializer to handle deserialization errors gracefully
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        
        // Configure the actual deserializers (delegates)
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
        
        // JacksonJsonDeserializer configuration
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
        config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.mhb.core.ProductCreatedEvent");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Dead Letter Publishing Recoverer - sends failed messages to DLT

        /* DeadLetterPublishingRecoverer class here is a spring apache kafka library used to send failed messages to dead letter topics
         kafkaTemplate will actually be used to send bad/dead messages to dead letter topics. because kafkaTemplate robs kafka producer and provide method to send producer messages to kafka topic.
         since kafkaTemplate knows how to send kafka messages to kafka topic, it is perfect tool for DeadLetterPublishingRecoverer to use when it need to send failed
         messages to dead letter topic*/
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, exception) -> {
                    // DLT naming strategy: original-topic-name.DLT
                    String deadLetterTopic = consumerRecord.topic() + ".DLT";
                    return new TopicPartition(deadLetterTopic, consumerRecord.partition());
                });
        
        /*use dead letter publishing recovery to publish failed messages to dead letter topics while error occur during message consumption by kafka listener.
         Retry 3 times with 1 second delay between retries, then publish to DLT*/
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(3000L, 3L));
        handler.addNotRetryableExceptions(NotRetryableException.class);
        handler.addRetryableExceptions(RetryableException.class);
        // Optionally, you can add exceptions that should NOT be retried (go straight to DLT)
        // handler.addNotRetryableExceptions(JsonProcessingException.class);
        
        return handler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String,Object> kafkaListenerContainerFactory(
            ConsumerFactory<String,Object> consumerFactory,
            CommonErrorHandler errorHandler){
        ConcurrentKafkaListenerContainerFactory<String,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
