package com.example.monir.EmailNotificationService.handler;

import com.example.mhb.core.ProductCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

/*    @KafkaListener(topics = {"product-created-event-topic","anc-topic"})
    a single kafka consumer can consume messages from multiple topics
     */
    @KafkaListener(topics = {"product-created-event-topic"})
    public void handle(
            @Payload ProductCreatedEvent event,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage){
        
        // Check if there was a deserialization error
        if (exceptionMessage != null) {
            LOGGER.error("Deserialization error from topic: {}, partition: {}, offset: {}. Error: {}", 
                    topic, partition, offset, exceptionMessage);
            // Handle the error (e.g., log to dead letter queue, send alert, etc.)
            return;
        }
        
        LOGGER.info("Received a new event: {} from topic: {}, partition: {}, offset: {}", 
                event.getTitle(), topic, partition, offset);
        
        // Process the event (send email notification, etc.)
    }
}