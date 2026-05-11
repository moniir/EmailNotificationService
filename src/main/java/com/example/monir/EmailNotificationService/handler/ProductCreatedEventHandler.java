package com.example.monir.EmailNotificationService.handler;

import com.example.mhb.core.ProductCreatedEvent;
import com.example.monir.EmailNotificationService.entity.ProcessedEventEntity;
import com.example.monir.EmailNotificationService.error.NotRetryableException;
import com.example.monir.EmailNotificationService.error.RetryableException;
import com.example.monir.EmailNotificationService.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Transactional
@Component
@KafkaListener(topics = {"product-created-event-topic"})
/*    @KafkaListener(topics = {"product-created-event-topic","anc-topic"})
      a single kafka consumer can consume messages from multiple topics
*/
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private RestTemplate restTemplate;
    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent event, @Header(value = "messageId") String messageId,
                       @Header(value = KafkaHeaders.RECEIVED_KEY) String messageKey) { // if we want either messageId or messageKey optional, we may use required = false
        LOGGER.info("Received a new event: " + event.getTitle());
        ProcessedEventEntity existingProcessedEvent = processedEventRepository.findByMessageId(messageId);
        if(existingProcessedEvent != null) {
            LOGGER.info("Found a duplicate message id: {}",existingProcessedEvent.getMessageId());
            return;
        }
        String reqUrl = "http://localhost:8888/response/200"; //tested using mock application
        try {
            ResponseEntity<String> response = restTemplate.exchange(reqUrl, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response from a remote service: " + response.getBody());
            }
        } catch (ResourceAccessException ex) {
            LOGGER.error(ex.getMessage());
            throw new RetryableException(ex);
        } catch (HttpServerErrorException ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }
        //saving messageId in db table
        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, event.getProductId()));
        } catch (DataIntegrityViolationException e) {
            throw new NotRetryableException(e);
        }
    }
}