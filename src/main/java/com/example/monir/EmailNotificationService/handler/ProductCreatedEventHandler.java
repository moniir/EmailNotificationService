package com.example.monir.EmailNotificationService.handler;

import com.example.mhb.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

/*    @KafkaListener(topics = {"product-created-event-topic","anc-topic"})
    a single kafka consumer can consume messages from multiple topics
     */
    @KafkaListener(topics = {"product-created-event-topic"})
    public void handle(ProductCreatedEvent event){
        LOGGER.info("Received a new event: "+event.getTitle());
    }
}