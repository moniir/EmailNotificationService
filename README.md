# Email Notification Service

A Spring Boot microservice that consumes product creation events from Apache Kafka and processes email notifications.

## Technologies

- **Spring Boot 4.x** (Spring Framework 7.x)
- **Spring Kafka 4.0.5** with `JacksonJsonDeserializer`
- **Apache Kafka 4.1.2**
- **Java 17+**
- **Maven**

## Features

- Kafka consumer that listens to `product-created-event-topic`
- JSON deserialization using Spring Kafka's `JacksonJsonDeserializer` (compatible with Spring Boot 4)
- Configurable consumer groups and bootstrap servers
- Handles `ProductCreatedEvent` from `com.example.mhb.core` package
- **Error Handling & Retry**: Automatic retry mechanism with configurable backoff
- **Dead Letter Topic (DLT)**: Failed messages published to `<original-topic>.DLT`
- **Event Persistence**: H2 database for tracking processed events (idempotency)
- **Custom Exception Handling**: RetryableException vs NotRetryableException logic

## Prerequisites

- JDK 17 or higher
- Apache Kafka running on `localhost:9092` and `localhost:9094`
- Maven 3.6+

## Configuration

### application.properties

```properties
# Server
server.port=53061

# Kafka Consumer
spring.kafka.consumer.bootstrap-servers=localhost:9092,localhost:9094
spring.kafka.consumer.group-id=product-created-events
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.mhb.core

# H2 Database (In-Memory)
spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driver-class-name=org.h2.Driver
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true
spring.datasource.username=monir
spring.datasource.password=monir
```

### H2 Database Console

Access the H2 console at: **http://localhost:53061/h2-console**

**Login credentials:**
- JDBC URL: `jdbc:h2:mem:testdb`
- Username: `monir`
- Password: `monir`

### Important Notes

- **Spring Boot 4 Compatibility**: This project uses `JacksonJsonDeserializer` instead of the deprecated `JsonDeserializer`
- **Trusted Packages**: Configure `spring.kafka.consumer.properties.spring.json.trusted.packages` to include packages containing your event classes
- **Default Type**: The consumer is configured to deserialize messages into `ProductCreatedEvent` by default
- **Error Handling**: Use `RetryableException` for transient errors and `NotRetryableException` for permanent failures
- **DLT Setup**: Create the Dead Letter Topic (`product-created-event-topic.DLT`) before running in production

## Running the Application

### 1. Start Kafka

Ensure Kafka is running on the configured bootstrap servers (default: `localhost:9092`, `localhost:9094`)

### 2. Build the Application

```bash
mvn clean package
```

### 3. Run the Application

```bash
mvn spring-boot:run
```

Or run the generated JAR:

```bash
java -jar target/EmailNotificationService-0.0.1-SNAPSHOT.jar
```

## Kafka Consumer Configuration

The Kafka consumer is configured in `KafkaConsumerConfiguration.java`:

- **Key Deserializer**: `StringDeserializer`
- **Value Deserializer**: `JacksonJsonDeserializer` (Spring Boot 4 compatible)
- **Default Type**: `com.example.mhb.core.ProductCreatedEvent`
- **Trusted Packages**: Configured via properties file

## Error Handling & Retry Mechanism

The service implements robust error handling with automatic retry and Dead Letter Topic (DLT) support:

### Retry Configuration
- **Retry Attempts**: 3 attempts with exponential backoff
- **Backoff**: 1 second initial delay, 2x multiplier, max 10 seconds
- **Retryable Errors**: `RetryableException` triggers retry logic
- **Non-Retryable Errors**: `NotRetryableException` immediately sends to DLT

### Dead Letter Topic (DLT)
- Failed messages (after all retries) are published to `product-created-event-topic.DLT`
- Create the DLT manually in Kafka or enable auto-topic creation
- Handled by `DeadLetterTopicHandler` for logging and monitoring

### Event Persistence
- `ProcessedEventEntity`: Tracks processed events in H2 database
- Enables idempotency and event deduplication
- Stores event metadata for audit trail

## Event Handler

The `ProductCreatedEventHandler` consumes messages from:
- `product-created-event-topic`

When a message is received, it logs the event title and can be extended to send email notifications.

## Development

### Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── com/example/monir/EmailNotificationService/
│   │       ├── EmailNotificationServiceApplication.java
│   │       ├── config/
│   │       │   └── KafkaConsumerConfiguration.java
│   │       ├── entity/
│   │       │   └── ProcessedEventEntity.java
│   │       ├── error/
│   │       │   ├── NotRetryableException.java
│   │       │   └── RetryableException.java
│   │       └── handler/
│   │           ├── ProductCreatedEventHandler.java
│   │           └── DeadLetterTopicHandler.java
│   └── resources/
│       └── application.properties
└── test/
    └── java/
        └── com/example/monir/EmailNotificationService/
            └── EmailNotificationServiceApplicationTests.java
```

## Troubleshooting

### H2 Database Connection Issues

**Error: "Database not found" or file path errors**
- Ensure JDBC URL in H2 console is exactly: `jdbc:h2:mem:testdb`
- Don't forget the `mem:` prefix for in-memory mode
- Restart the application after changing database configuration

### "No type information in headers and no default type provided"

This error occurs when `JacksonJsonDeserializer` cannot determine the target type. Ensure:
1. `VALUE_DEFAULT_TYPE` is configured in `KafkaConsumerConfiguration`
2. The default type matches the event class your handler expects

### Deserialization Errors

- Verify `spring.json.trusted.packages` includes the package containing your event classes
- Ensure the JSON structure in Kafka messages matches your event class structure
- Check that Jackson can deserialize your event class (needs default constructor, getters/setters)

### Dead Letter Topic Not Receiving Messages

- Ensure the DLT exists: `product-created-event-topic.DLT`
- Check Kafka logs for publishing errors
- Verify `DeadLetterPublishingRecoverer` is configured correctly
- Monitor DLT handler logs for received messages

## License

[Your License Here]
