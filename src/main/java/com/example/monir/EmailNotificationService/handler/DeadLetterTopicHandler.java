package com.example.monir.EmailNotificationService.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Topic (DLT) Handler
 * 
 * Listens to the DLT and logs/processes failed messages for:
 * - Manual inspection and debugging
 * - Alerting and monitoring
 * - Reprocessing logic
 * - Storing in database for later analysis
 * 
 * Note: Enable this listener by uncommenting @Component
 */
// @Component  // Uncomment to enable DLT listener
public class DeadLetterTopicHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {"product-created-event-topic.DLT"}, groupId = "dlt-handler-group")
    public void handleDeadLetterMessage(
            ConsumerRecord<String, Object> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(value = KafkaHeaders.EXCEPTION_STACKTRACE, required = false) String stacktrace) {

        LOGGER.error("=== Dead Letter Message Received ===");
        LOGGER.error("Original Topic: {}", record.key());
        LOGGER.error("DLT Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        LOGGER.error("Failed Message Key: {}", record.key());
        LOGGER.error("Failed Message Value: {}", record.value());
        LOGGER.error("Exception: {}", exceptionMessage);
        
        if (stacktrace != null) {
            LOGGER.error("Stacktrace: {}", stacktrace);
        }
        
        // TODO: Implement your DLT handling logic here:
        // - Store in database for manual review
        // - Send alert to monitoring system (e.g., PagerDuty, Slack)
        // - Attempt manual reprocessing with different logic
        // - Log to external system for auditing
        
        LOGGER.error("===================================");
    }
}
