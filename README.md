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

## Prerequisites

- JDK 17 or higher
- Apache Kafka running on `localhost:9092` and `localhost:9094`
- Maven 3.6+

## Configuration

### application.properties

```properties
server.port=53061
spring.kafka.consumer.bootstrap-servers=localhost:9092,localhost:9094
spring.kafka.consumer.group-id=product-created-events
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.mhb.core
```

### Important Notes

- **Spring Boot 4 Compatibility**: This project uses `JacksonJsonDeserializer` instead of the deprecated `JsonDeserializer`
- **Trusted Packages**: Configure `spring.kafka.consumer.properties.spring.json.trusted.packages` to include packages containing your event classes
- **Default Type**: The consumer is configured to deserialize messages into `ProductCreatedEvent` by default

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
│   │       └── handler/
│   │           └── ProductCreatedEventHandler.java
│   └── resources/
│       └── application.properties
└── test/
    └── java/
        └── com/example/monir/EmailNotificationService/
            └── EmailNotificationServiceApplicationTests.java
```

## Troubleshooting

### "No type information in headers and no default type provided"

This error occurs when `JacksonJsonDeserializer` cannot determine the target type. Ensure:
1. `VALUE_DEFAULT_TYPE` is configured in `KafkaConsumerConfiguration`
2. The default type matches the event class your handler expects

### Deserialization Errors

- Verify `spring.json.trusted.packages` includes the package containing your event classes
- Ensure the JSON structure in Kafka messages matches your event class structure
- Check that Jackson can deserialize your event class (needs default constructor, getters/setters)

## License

[Your License Here]
