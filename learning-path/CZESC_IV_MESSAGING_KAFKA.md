# 🎓 CZĘŚĆ IV: Messaging & Kafka (Tematy 31-40)

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + **Apache Kafka 7.6** + Spring Boot 3.4 + Nuxt 3  
**Cel:** Zaawansowane wzorce asynchronicznej komunikacji  
**Czas nauki:** 3-4 tygodnie  
**Wymagania:** Ukończone [CZĘŚĆ I](./CZESC_I_FUNDAMENTY.md), [CZĘŚĆ II](./CZESC_II_CQRS_W_PRAKTYCE.md), [CZĘŚĆ III](./CZESC_III_EVENT_SOURCING.md)

---

## 📋 Spis Treści - CZĘŚĆ IV

31. [Apache Kafka - Wprowadzenie](#temat-31-apache-kafka-wprowadzenie)
32. [Topics & Partitions](#temat-32-topics-partitions)
33. [Producers - Wysyłanie Messages](#temat-33-producers-wysylanie-messages)
34. [Consumers & Consumer Groups](#temat-34-consumers-consumer-groups)
35. [Offset Management](#temat-35-offset-management)
36. [Partitioning Strategy](#temat-36-partitioning-strategy)
37. [Error Handling & Dead Letter Queue](#temat-37-error-handling-dead-letter-queue)
38. [Exactly-Once Semantics](#temat-38-exactly-once-semantics)
39. [Schema Registry & Event Versioning](#temat-39-schema-registry-event-versioning)
40. [Transactional Outbox Pattern](#temat-40-transactional-outbox-pattern)

---

## 🎯 Cele Nauki - CZĘŚĆ IV

Po ukończeniu Części IV będziesz:
- ✅ Rozumieć architekturę Apache Kafka
- ✅ Implementować Producers i Consumers
- ✅ Zarządzać partycjonowaniem i offsets
- ✅ Obsługiwać błędy (DLQ, retry)
- ✅ Implementować Exactly-Once Semantics
- ✅ Wersjonować events (Schema Registry)
- ✅ Gwarantować spójność (Transactional Outbox)

---

## Temat 31: Apache Kafka - Wprowadzenie

### 31.1 Definicja

**Apache Kafka** to distributed streaming platform działający jak **dziennik** (log) dla events.

### 31.2 Analogia: Tablica Ogłoszeń

**Tradycyjne API (REST):**
```
Service A → bezpośredni call → Service B
- Jeśli B nie działa = A nie działa
- A musi znać adres B
- Synchroniczne (czekanie na odpowiedź)
```

**Kafka (Message Broker):**
```
Service A → message → Kafka (tablica) → Service B
- Jeśli B nie działa = message czeka
- A nie zna B (loose coupling)
- Asynchroniczne (fire & forget)
```

### 31.3 Architektura Kafka

```
┌─────────────────────────────────────────────────────────┐
│                    KAFKA CLUSTER                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Topic: events.lifecycle                          │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐       │  │
│  │  │Partition │  │Partition │  │Partition │       │  │
│  │  │    0     │  │    1     │  │    2     │       │  │
│  │  ├──────────┤  ├──────────┤  ├──────────┤       │  │
│  │  │ offset 0 │  │ offset 0 │  │ offset 0 │       │  │
│  │  │ offset 1 │  │ offset 1 │  │ offset 1 │       │  │
│  │  │ offset 2 │  │ offset 2 │  │ offset 2 │       │  │
│  │  │   ...    │  │   ...    │  │   ...    │       │  │
│  │  └──────────┘  └──────────┘  └──────────┘       │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
              ↑                           ↓
         ┌─────────┐               ┌─────────┐
         │Producer │               │Consumer │
         │(Backend)│               │(Projector)│
         └─────────┘               └─────────┘
```

### 31.4 Kluczowe Koncepcje

**Topic:**
- Kategoria/kanał dla messages
- Np. "events.lifecycle", "bookings.created"

**Partition:**
- Topic jest podzielony na partitions dla równoległości
- Każda partition to uporządkowany, niezmienny log

**Offset:**
- Pozycja message w partition
- Liczba całkowita (0, 1, 2, 3, ...)

**Producer:**
- Aplikacja wysyłająca messages do Kafka

**Consumer:**
- Aplikacja czytająca messages z Kafka

**Broker:**
- Serwer Kafka (zazwyczaj klaster 3+ brokerów)

### 31.5 Kafka vs Tradycyjne Message Queues

| Feature | RabbitMQ (Queue) | Apache Kafka (Log) |
|---------|------------------|-------------------|
| **Model** | Queue (FIFO) | Append-only log |
| **Po odczycie** | Message deleted after read | Message retained |
| **Replay** | ❌ No | ✅ Yes (rewind offset) |
| **Ordering** | Per queue | Per partition |
| **Throughput** | ~50k msg/s | ~1M+ msg/s |
| **Use case** | Task queue | Event streaming |

### 31.6 Docker Compose - Kafka Setup (KRaft Mode)

```yaml
# docker-compose.yml
version: '3.8'

services:
  kafka:
    image: apache/kafka:7.6.0
    container_name: eventmaster-kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      # KRaft mode (bez Zookeeper!)
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      
      # Cluster config
      CLUSTER_ID: 'EventMasterKafkaCluster'
      
      # Topic defaults
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      
      # Retention
      KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
      KAFKA_LOG_RETENTION_BYTES: -1   # Unlimited
      
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - eventmaster-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: eventmaster-kafka-ui
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: eventmaster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      - kafka
    networks:
      - eventmaster-network

volumes:
  kafka_data:

networks:
  eventmaster-network:
    driver: bridge
```

### 31.7 Spring Boot Configuration

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all  # Wait for all replicas
      retries: 3
      
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      group-id: eventmaster-backend
      auto-offset-reset: earliest
      enable-auto-commit: false  # Manual commit
      properties:
        spring.json.trusted.packages: com.eventmaster.events
        
    listener:
      ack-mode: manual  # Manual acknowledge
```

### 31.8 Topic Configuration

```java
@Configuration
public class KafkaTopicConfig {
    
    @Bean
    public NewTopic eventsLifecycleTopic() {
        return TopicBuilder.name("events.lifecycle")
            .partitions(3)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")  // 7 days
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
            .build();
    }
    
    @Bean
    public NewTopic bookingsCreatedTopic() {
        return TopicBuilder.name("bookings.created")
            .partitions(3)
            .replicas(1)
            .build();
    }
    
    @Bean
    public NewTopic paymentsProcessedTopic() {
        return TopicBuilder.name("payments.processed")
            .partitions(3)
            .replicas(1)
            .build();
    }
}
```

### 31.9 Testowanie Kafka z Testcontainers

```java
@SpringBootTest
@Testcontainers
class KafkaIntegrationTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("apache/kafka:7.6.0")
    );
    
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18");
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Test
    void shouldConnectToKafka() {
        // Given
        String topic = "test-topic";
        String message = "Hello Kafka!";
        
        // When
        kafkaTemplate.send(topic, message).join();
        
        // Then
        // Message wysłana (sprawdzone przez join() bez exception)
    }
}
```

### 31.10 Kafka CLI Commands

```bash
# Lista topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Opis topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events.lifecycle

# Konsumuj messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic events.lifecycle --from-beginning

# Produkuj message
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic events.lifecycle

# Consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Consumer group details
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group eventmaster-backend
```

### 31.11 Zalety Kafka

**1. Durability (Trwałość):**
- Messages zapisane na dysku
- Replicas dla redundancy
- Nie gubi danych

**2. Scalability (Skalowalność):**
- Horizontal scaling (add more brokers)
- Partitioning dla równoległości
- Millions msg/s

**3. Replayability (Odtwarzalność):**
- Messages nie są usuwane po odczycie
- Consumer może "cofnąć" offset
- Event sourcing friendly!

**4. Decoupling (Luźne Powiązanie):**
- Producer nie zna Consumers
- Add/remove consumers bez zmian w Producer

### 31.12 Wady Kafka

**1. Complexity (Złożoność):**
- Requires Operational knowledge
- Monitoring, tuning, maintenance

**2. Eventual Consistency:**
- Messages nie arrive natychmiast
- Trzeba handle delays

**3. Ordering:**
- Tylko w ramach partition
- Nie ma global ordering

**4. Over-engineering:**
- Dla prostych CRUD może być overkill
- RabbitMQ może wystarczyć

### 31.13 Kiedy Używać Kafka?

**✅ UŻYJ gdy:**
- Event streaming (real-time data)
- High throughput (100k+ msg/s)
- Event Sourcing / CQRS
- Log aggregation
- Multiple consumers tego samego streamu

**❌ NIE UŻYWAJ gdy:**
- Prosty request-response (użyj REST)
- Low latency critical (<10ms)
- Mały zespół bez Ops experience
- Prototypowanie

### 31.14 Podsumowanie Tematu 31

**Kluczowe Pojęcia:**
- Kafka = distributed log
- Topic → Partitions → Offsets
- Producer → Kafka → Consumer
- KRaft mode (bez Zookeeper)

**W EventMaster:**
- Docker Compose setup
- Spring Kafka configuration
- Topic creation
- Testcontainers dla testów

**Następny temat:** [Topics & Partitions](#temat-32-topics-partitions)

---

## Temat 32: Topics & Partitions

### 32.1 Definicja

**Topic** = logiczna kategoria dla messages (jak folder)  
**Partition** = fizyczny podział topic dla skalowalności (jak pliki w folderze)

### 32.2 Anatomia Topic

```
Topic: events.lifecycle (3 partitions)

Partition 0: [msg0] [msg1] [msg2] [msg3] [msg4] ...
             offset 0   1     2     3     4

Partition 1: [msg0] [msg1] [msg2] ...
             offset 0   1     2

Partition 2: [msg0] [msg1] [msg2] [msg3] ...
             offset 0   1     2     3
```

**Właściwości:**
- Messages w partition są **ordered** (uporządkowane)
- Messages między partitions NIE są ordered
- Każda partition ma niezależne offsets

### 32.3 Dlaczego Partitions?

**1. Równoległość (Parallelism):**
```
1 Partition  → 1 Consumer  → 1000 msg/s
3 Partitions → 3 Consumers → 3000 msg/s
10 Partitions → 10 Consumers → 10,000 msg/s
```

**2. Skalowalność (Scalability):**
- Każda partition może być na innym brokerze
- Load jest rozłożony

**3. Fault Tolerance:**
- Partition replicas na różnych brokerach
- If broker fails → replica takes over

### 32.4 Naming Convention

```java
// ✅ GOOD: Domain-driven naming
"events.lifecycle"           // Event management events
"bookings.created"           // Booking created events
"payments.processed"         // Payment processed events
"notifications.email.sent"   // Email notification events

// ❌ BAD: Technical naming
"topic1"
"events"
"data"
```

### 32.5 Partition Count - Ile Partycji?

**Rule of thumb:**
```
Partitions = Expected Throughput / Consumer Throughput

Example:
Expected: 60,000 msg/s
Consumer: 20,000 msg/s
Partitions = 60,000 / 20,000 = 3
```

**Recommendations EventMaster:**
- **Low traffic** (<1k msg/s): 3 partitions
- **Medium traffic** (1k-10k msg/s): 6 partitions
- **High traffic** (>10k msg/s): 12+ partitions

**⚠️ Warning:** Więcej partitions = więcej resources (file descriptors, memory)

### 32.6 Topic Configuration w Spring Boot

```java
@Configuration
public class KafkaTopicConfig {
    
    /**
     * Events lifecycle topic
     * - Partitions: 3 (medium traffic expected)
     * - Retention: 7 days
     * - Cleanup: delete old messages
     */
    @Bean
    public NewTopic eventsLifecycleTopic() {
        return TopicBuilder.name("events.lifecycle")
            .partitions(3)
            .replicas(1)  // 1 replica w dev, 3 w prod
            .config(TopicConfig.RETENTION_MS_CONFIG, 
                String.valueOf(Duration.ofDays(7).toMillis()))
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, 
                TopicConfig.CLEANUP_POLICY_DELETE)
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            .build();
    }
    
    /**
     * Bookings created topic
     * - Higher partitions (expected high traffic)
     * - Longer retention (audit trail)
     */
    @Bean
    public NewTopic bookingsCreatedTopic() {
        return TopicBuilder.name("bookings.created")
            .partitions(6)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,
                String.valueOf(Duration.ofDays(30).toMillis()))
            .build();
    }
    
    /**
     * DLQ (Dead Letter Queue) topic
     * - Low partitions (errors are rare)
     * - Long retention (manual review)
     */
    @Bean
    public NewTopic deadLetterQueueTopic() {
        return TopicBuilder.name("dlq.events")
            .partitions(1)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG,
                String.valueOf(Duration.ofDays(90).toMillis()))
            .build();
    }
}
```

### 32.7 Retention Policies

**Delete (domyślnie):**
```java
// Messages starsze niż X są usuwane
.config(TopicConfig.RETENTION_MS_CONFIG, "604800000")  // 7 dni
.config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
```

**Compact:**
```java
// Tylko ostatnia message dla każdego klucza jest zachowana
.config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")

// Use case: User profiles, Configuration
// Key: userId → najnowszy profil
```

**Delete + Compact:**
```java
.config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,compact")
// Usuń stare + compact aktywne
```

### 32.8 Testowanie Topics

```java
@SpringBootTest
@Testcontainers
class KafkaTopicTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("apache/kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private AdminClient adminClient;
    
    @Test
    void topicsShouldBeCreated() throws Exception {
        // Given
        Set<String> expectedTopics = Set.of(
            "events.lifecycle",
            "bookings.created",
            "payments.processed"
        );
        
        // When
        Set<String> actualTopics = adminClient.listTopics()
            .names()
            .get(5, TimeUnit.SECONDS);
        
        // Then
        assertThat(actualTopics).containsAll(expectedTopics);
    }
    
    @Test
    void eventsTopicShouldHave3Partitions() throws Exception {
        // When
        TopicDescription desc = adminClient
            .describeTopics(List.of("events.lifecycle"))
            .values()
            .get("events.lifecycle")
            .get();
        
        // Then
        assertThat(desc.partitions()).hasSize(3);
    }
    
    @Test
    void topicShouldHaveCorrectRetention() throws Exception {
        // When
        ConfigResource resource = new ConfigResource(
            ConfigResource.Type.TOPIC, 
            "events.lifecycle"
        );
        
        Config config = adminClient
            .describeConfigs(List.of(resource))
            .values()
            .get(resource)
            .get();
        
        String retention = config.get(TopicConfig.RETENTION_MS_CONFIG).value();
        
        // Then
        assertThat(retention).isEqualTo("604800000");  // 7 days
    }
}
```

### 32.9 Monitoring Topics

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaTopicMonitoring {
    
    private final AdminClient adminClient;
    private final MeterRegistry meterRegistry;
    
    @Scheduled(fixedDelay = 60000)  // Every minute
    public void monitorTopics() {
        try {
            Map<String, TopicDescription> topics = adminClient
                .describeTopics(List.of("events.lifecycle", "bookings.created"))
                .allTopicNames()
                .get();
            
            topics.forEach((name, desc) -> {
                int partitions = desc.partitions().size();
                
                meterRegistry.gauge(
                    "kafka.topic.partitions",
                    Tags.of("topic", name),
                    partitions
                );
                
                log.info("Topic: {} has {} partitions", name, partitions);
            });
            
        } catch (Exception e) {
            log.error("Failed to monitor topics", e);
        }
    }
}
```

### 32.10 Best Practices

**1. Immutable Topics:**
- Po utworzeniu nie zmieniaj partitions count (break consumers!)
- If needed: create new topic, migrate

**2. Naming:**
- Use domain language: "events.lifecycle", not "topic1"
- Hierarchical: "domain.subdomain.action"

**3. Retention:**
- Events: 7-30 days
- Audit logs: 90+ days
- DLQ: long (manual review)

**4. Partitions:**
- Start with 3-6
- Monitor lag, increase if needed
- Don't over-partition (resource waste)

### 32.11 Podsumowanie Tematu 32

**Kluczowe Pojęcia:**
- Topic = category
- Partition = physical split
- Offset = position in partition
- Retention = how long to keep

**Configuration:**
- TopicBuilder API
- Retention policies (delete, compact)
- Partition count planning
- Testing with AdminClient

**Następny temat:** [Producers - Wysyłanie Messages](#temat-33-producers-wysylanie-messages)

---

## Temat 33: Producers - Wysyłanie Messages

### 33.1 Definicja

**Producer** to aplikacja wysyłająca messages do Kafka topics.

### 33.2 Producer Architecture

```
EventMaster Backend
      ↓
KafkaTemplate
      ↓
ProducerFactory
      ↓
KafkaProducer (Apache Kafka client)
      ↓
Serializer (Java Object → bytes)
      ↓
Partitioner (wybór partition)
      ↓
Record Batch (grupowanie messages)
      ↓
Network Send → Kafka Broker
```

### 33.3 Simple Producer

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublisher {
    
    private final KafkaTemplate<String, EventCreatedEvent> kafkaTemplate;
    
    /**
     * Publish EventCreatedEvent
     */
    public void publishEventCreated(EventCreatedEvent event) {
        String topic = "events.lifecycle";
        String key = event.getEventId().toString();
        
        // Send & forget (fire and forget)
        kafkaTemplate.send(topic, key, event);
        
        log.info("Published EventCreatedEvent: eventId={}", event.getEventId());
    }
}
```

### 33.4 Producer z Confirmation

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ReliableEventPublisher {
    
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    /**
     * Publish with confirmation
     */
    public CompletableFuture<SendResult<String, DomainEvent>> publishEvent(
        String topic,
        DomainEvent event
    ) {
        String key = extractKey(event);
        
        return kafkaTemplate.send(topic, key, event)
            .thenApply(result -> {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Message sent: topic={}, partition={}, offset={}", 
                    metadata.topic(), 
                    metadata.partition(), 
                    metadata.offset()
                );
                return result;
            })
            .exceptionally(ex -> {
                log.error("Failed to send message: topic={}, event={}", topic, event, ex);
                throw new MessagePublishException("Failed to publish event", ex);
            });
    }
    
    private String extractKey(DomainEvent event) {
        if (event instanceof EventCreatedEvent e) {
            return e.getEventId().toString();
        } else if (event instanceof BookingCreatedEvent b) {
            return b.getBookingId().toString();
        }
        return UUID.randomUUID().toString();
    }
}
```

### 33.5 Producer Configuration

```yaml
# application.yml
spring:
  kafka:
    producer:
      # Serializers
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      
      # Acknowledgments
      acks: all  # all = wait for all in-sync replicas (safest)
      # acks: 0 = fire and forget (fastest, but unsafe)
      # acks: 1 = wait for leader (middle ground)
      
      # Retries
      retries: 3
      retry-backoff-ms: 1000
      
      # Batching
      batch-size: 16384  # 16 KB
      linger-ms: 10      # Wait 10ms to batch
      
      # Compression
      compression-type: snappy  # snappy, gzip, lz4, zstd
      
      # Idempotence
      enable-idempotence: true
      
      # Timeout
      request-timeout-ms: 30000
      
      properties:
        # Max request size
        max.request.size: 1048576  # 1 MB
```

### 33.6 Custom Producer Factory

```java
@Configuration
public class KafkaProducerConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Bean
    public ProducerFactory<String, DomainEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Reliability
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Performance
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new DefaultKafkaProducerFactory<>(config);
    }
    
    @Bean
    public KafkaTemplate<String, DomainEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

### 33.7 Message Headers

```java
@Service
@RequiredArgsConstructor
public class EventPublisherWithHeaders {
    
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    public void publish(DomainEvent event) {
        ProducerRecord<String, DomainEvent> record = new ProducerRecord<>(
            "events.lifecycle",
            null,  // partition (null = auto)
            event.getEventId().toString(),
            event
        );
        
        // Add headers
        record.headers()
            .add("event-type", event.getClass().getSimpleName().getBytes())
            .add("event-version", "1".getBytes())
            .add("correlation-id", event.getCorrelationId().toString().getBytes())
            .add("causation-id", event.getCausationId().toString().getBytes())
            .add("user-id", event.getUserId().toString().getBytes())
            .add("timestamp", event.getOccurredAt().toString().getBytes());
        
        kafkaTemplate.send(record);
    }
}
```

### 33.8 Producer Interceptor

```java
public class EventPublishInterceptor implements ProducerInterceptor<String, DomainEvent> {
    
    private static final Logger log = LoggerFactory.getLogger(EventPublishInterceptor.class);
    
    @Override
    public ProducerRecord<String, DomainEvent> onSend(ProducerRecord<String, DomainEvent> record) {
        log.debug("Sending: topic={}, key={}, value={}", 
            record.topic(), record.key(), record.value().getClass().getSimpleName());
        
        // Add timestamp header if missing
        if (!hasHeader(record, "timestamp")) {
            record.headers().add("timestamp", 
                Instant.now().toString().getBytes());
        }
        
        return record;
    }
    
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("Failed to send: topic={}, partition={}", 
                metadata.topic(), metadata.partition(), exception);
        } else {
            log.debug("Ack received: topic={}, partition={}, offset={}", 
                metadata.topic(), metadata.partition(), metadata.offset());
        }
    }
    
    @Override
    public void close() {
        // Cleanup resources
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration
    }
    
    private boolean hasHeader(ProducerRecord<?, ?> record, String key) {
        return record.headers().lastHeader(key) != null;
    }
}
```

### 33.9 Testowanie Producer

```java
@SpringBootTest
@Testcontainers
@EmbeddedKafka
class EventPublisherTest {
    
    @Autowired
    private EventPublisher publisher;
    
    @Autowired
    private KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    @Test
    void shouldPublishEventToKafka() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "JavaConf 2025",
            "Warsaw",
            Instant.now()
        );
        
        // When
        publisher.publishEventCreated(event);
        
        // Then
        // Use EmbeddedKafka to consume
        ConsumerRecord<String, EventCreatedEvent> record = 
            KafkaTestUtils.getSingleRecord(
                consumer, 
                "events.lifecycle",
                Duration.ofSeconds(5)
            );
        
        assertThat(record.key()).isEqualTo(event.getEventId().toString());
        assertThat(record.value().getName()).isEqualTo("JavaConf 2025");
    }
    
    @Test
    void shouldHandlePublishError() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(/* ... */);
        
        // Mock Kafka failure
        doThrow(new RuntimeException("Kafka down"))
            .when(kafkaTemplate).send(anyString(), any(), any());
        
        // When / Then
        assertThatThrownBy(() -> publisher.publishEventCreated(event))
            .isInstanceOf(MessagePublishException.class);
    }
}
```

### 33.10 Producer Metrics

```java
@Service
@RequiredArgsConstructor
public class ProducerMetrics {
    
    private final MeterRegistry meterRegistry;
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    public void publishWithMetrics(String topic, DomainEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            kafkaTemplate.send(topic, event).get();
            
            meterRegistry.counter(
                "kafka.producer.messages.sent",
                "topic", topic,
                "event_type", event.getClass().getSimpleName()
            ).increment();
            
        } catch (Exception e) {
            meterRegistry.counter(
                "kafka.producer.messages.failed",
                "topic", topic,
                "error", e.getClass().getSimpleName()
            ).increment();
            
            throw new MessagePublishException("Failed to publish", e);
        } finally {
            sample.stop(meterRegistry.timer(
                "kafka.producer.send.duration",
                "topic", topic
            ));
        }
    }
}
```

### 33.11 Podsumowanie Tematu 33

**Kluczowe Pojęcia:**
- Producer = wysyła messages
- KafkaTemplate = Spring abstraction
- Serializer = Java → bytes
- Acks = confirmation level

**Best Practices:**
- acks=all dla reliability
- enable-idempotence=true
- Add headers (correlation-id, etc.)
- Monitor metrics

**Następny temat:** [Consumers & Consumer Groups](#temat-34-consumers-consumer-groups)

---

## Temat 34: Consumers & Consumer Groups

### 34.1 Definicja

**Consumer** = aplikacja czytająca messages z Kafka  
**Consumer Group** = grupa consumers dzieląca się pracą

### 34.2 Consumer Group Architecture

```
Topic: events.lifecycle (3 partitions)
├─ Partition 0: [msg0] [msg1] [msg2] ...
├─ Partition 1: [msg0] [msg1] [msg2] ...
└─ Partition 2: [msg0] [msg1] [msg2] ...

Consumer Group: "eventmaster-projectors"
├─ Consumer 1 → reads Partition 0
├─ Consumer 2 → reads Partition 1
└─ Consumer 3 → reads Partition 2

Każda partition jest czytana przez JEDNEGO consumera w grupie
```

### 34.3 Simple Consumer

```java
@Service
@Slf4j
public class EventConsumer {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors"
    )
    public void consume(EventCreatedEvent event) {
        log.info("Received EventCreatedEvent: {}", event.getEventId());
        
        // Process event
        // ...
    }
}
```

### 34.4 Consumer z Manual Acknowledgment

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ReliableEventConsumer {
    
    private final EventProjector projector;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
        ConsumerRecord<String, EventCreatedEvent> record,
        Acknowledgment ack
    ) {
        log.info("Consuming: topic={}, partition={}, offset={}, key={}", 
            record.topic(), record.partition(), record.offset(), record.key());
        
        try {
            // Process event
            projector.project(record.value());
            
            // Manual commit (only after successful processing!)
            ack.acknowledge();
            
            log.info("Processed successfully: offset={}", record.offset());
            
        } catch (Exception e) {
            log.error("Failed to process: offset={}", record.offset(), e);
            // Don't acknowledge = message will be reprocessed
            throw e;
        }
    }
}
```

### 34.5 Consumer Configuration

```yaml
# application.yml
spring:
  kafka:
    consumer:
      # Deserializers
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      
      # Consumer Group
      group-id: eventmaster-backend
      
      # Offset reset strategy
      auto-offset-reset: earliest  # earliest, latest, none
      
      # Commit strategy
      enable-auto-commit: false  # Manual commit
      
      # Fetch size
      max-poll-records: 500
      fetch-min-bytes: 1
      fetch-max-wait-ms: 500
      
      # Session timeout
      session-timeout-ms: 30000
      heartbeat-interval-ms: 3000
      
      properties:
        # JSON deserializer config
        spring.json.trusted.packages: com.eventmaster.events
        spring.json.type.mapping: >
          EventCreatedEvent:com.eventmaster.events.EventCreatedEvent,
          EventPublishedEvent:com.eventmaster.events.EventPublishedEvent
    
    listener:
      # Acknowledgment mode
      ack-mode: manual  # manual, manual_immediate, record, batch
      
      # Concurrency (threads per consumer)
      concurrency: 3
      
      # Error handling
      type: batch  # single, batch
```

### 34.6 Custom Consumer Factory

```java
@Configuration
public class KafkaConsumerConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Bean
    public ConsumerFactory<String, DomainEvent> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "eventmaster-projectors");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        // Offset management
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Performance
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        
        // JSON deserializer
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.eventmaster.events");
        
        return new DefaultKafkaConsumerFactory<>(config);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DomainEvent> 
        kafkaListenerContainerFactory() {
        
        ConcurrentKafkaListenerContainerFactory<String, DomainEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);  // 3 threads
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        
        return factory;
    }
}
```

### 34.7 Multiple Consumers - Different Groups

```java
// Consumer Group 1: Projectors
@Service
@Slf4j
public class ProjectorConsumer {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors"  // Group 1
    )
    public void projectEvent(EventCreatedEvent event) {
        log.info("[Projector] Processing: {}", event.getEventId());
        // Update Read Model
    }
}

// Consumer Group 2: Email Service
@Service
@Slf4j
public class EmailConsumer {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-email-service"  // Group 2 (different!)
    )
    public void sendEmail(EventCreatedEvent event) {
        log.info("[Email] Processing: {}", event.getEventId());
        // Send confirmation email
    }
}

// Consumer Group 3: Analytics
@Service
@Slf4j
public class AnalyticsConsumer {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-analytics"  // Group 3
    )
    public void trackEvent(EventCreatedEvent event) {
        log.info("[Analytics] Processing: {}", event.getEventId());
        // Track in analytics DB
    }
}

// Każda grupa dostanie WSZYSTKIE messages!
// Wewnątrz grupy messages są rozdzielane między consumers
```

### 34.8 Batch Consumer

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchEventConsumer {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-batch-projectors",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatch(
        List<ConsumerRecord<String, EventCreatedEvent>> records,
        Acknowledgment ack
    ) {
        log.info("Received batch of {} messages", records.size());
        
        try {
            // Process batch
            List<EventView> views = records.stream()
                .map(record -> toEventView(record.value()))
                .collect(Collectors.toList());
            
            // Batch insert (faster than individual inserts!)
            eventViewRepository.saveAll(views);
            
            // Commit batch
            ack.acknowledge();
            
            log.info("Batch processed: {} events", views.size());
            
        } catch (Exception e) {
            log.error("Batch processing failed", e);
            throw e;
        }
    }
}
```

### 34.9 Consumer Rebalancing

**Scenario:** Add/remove consumer

```
Initial State (2 consumers, 3 partitions):
Consumer 1 → Partition 0, 1
Consumer 2 → Partition 2

Add Consumer 3:
Kafka triggers REBALANCE

New State (3 consumers, 3 partitions):
Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2

Perfect balance! ✅
```

**Rebalance Listener:**
```java
@Service
@Slf4j
public class EventConsumerWithRebalanceListener {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors"
    )
    public void consume(EventCreatedEvent event, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Processing event from partition: {}", partition);
    }
    
    @Bean
    public ConsumerAwareRebalanceListener rebalanceListener() {
        return new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions assigned: {}", partitions);
            }
            
            @Override
            public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions revoked: {}", partitions);
                // Commit pending work before revocation
            }
        };
    }
}
```

### 34.10 Testowanie Consumer

```java
@SpringBootTest
@Testcontainers
@EmbeddedKafka(partitions = 3, topics = "events.lifecycle")
class EventConsumerTest {
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> kafkaTemplate;
    
    @Autowired
    private EventViewRepository repository;
    
    @Test
    void consumerShouldProcessMessage() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "Test Event",
            "Warsaw",
            Instant.now()
        );
        
        // When
        kafkaTemplate.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<EventView> views = repository.findAll();
            assertThat(views).hasSize(1);
            assertThat(views.get(0).getName()).isEqualTo("Test Event");
        });
    }
    
    @Test
    void multipleConsumersShouldSharePartitions() {
        // Given: 3 partitions, 3 consumers in group
        // Each consumer gets 1 partition
        
        // When: Send 300 messages
        for (int i = 0; i < 300; i++) {
            EventCreatedEvent event = createEvent(i);
            kafkaTemplate.send("events.lifecycle", String.valueOf(i), event);
        }
        
        // Then: All 300 processed (by 3 consumers together)
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(repository.count()).isEqualTo(300);
        });
    }
}
```

### 34.11 Consumer Metrics

```java
@Service
@RequiredArgsConstructor
public class ConsumerMetrics {
    
    private final MeterRegistry meterRegistry;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors"
    )
    public void consumeWithMetrics(
        ConsumerRecord<String, EventCreatedEvent> record,
        Acknowledgment ack
    ) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Process
            process(record.value());
            
            // Metrics: success
            meterRegistry.counter(
                "kafka.consumer.messages.processed",
                "topic", record.topic(),
                "partition", String.valueOf(record.partition())
            ).increment();
            
            ack.acknowledge();
            
        } catch (Exception e) {
            // Metrics: failure
            meterRegistry.counter(
                "kafka.consumer.messages.failed",
                "topic", record.topic(),
                "error", e.getClass().getSimpleName()
            ).increment();
            
            throw e;
        } finally {
            sample.stop(meterRegistry.timer(
                "kafka.consumer.processing.duration",
                "topic", record.topic()
            ));
        }
    }
}
```

### 34.12 Podsumowanie Tematu 34

**Kluczowe Pojęcia:**
- Consumer = reads messages
- Consumer Group = sharing work
- Rebalancing = partition redistribution
- Manual commit = reliability

**Best Practices:**
- Use consumer groups
- Manual acknowledgment
- Batch processing dla performance
- Monitor lag

**Następny temat:** [Offset Management](#temat-35-offset-management)

---

## Temat 35: Offset Management

### 35.1 Definicja

**Offset** = pozycja consumera w partition (który message został ostatnio przeczytany)

### 35.2 Offset Anatomy

```
Partition 0:
[msg0] [msg1] [msg2] [msg3] [msg4] [msg5] ...
  0      1      2      3      4      5

Consumer offset = 3
    ↑
    Ostatnio przeczytany: msg2
    Następny do odczytu: msg3
```

### 35.3 Commit Strategies

**1. Auto Commit (domyślnie OFF w EventMaster):**
```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-commit-interval: 5000  # 5 seconds
```

```
Message received → Process → (wait 5s) → Auto commit
Problem: Jeśli crash między receive a commit = message lost! 😱
```

**2. Manual Commit (ZALECANE):**
```java
@KafkaListener(topics = "events.lifecycle")
public void consume(ConsumerRecord<String, Event> record, Acknowledgment ack) {
    try {
        process(record.value());
        ack.acknowledge();  // Commit TYLKO po sukcesie! ✅
    } catch (Exception e) {
        // Don't commit = message will be reprocessed
        throw e;
    }
}
```

**3. Batch Manual Commit:**
```java
@KafkaListener(topics = "events.lifecycle")
public void consumeBatch(
    List<ConsumerRecord<String, Event>> records,
    Acknowledgment ack
) {
    try {
        processBatch(records);
        ack.acknowledge();  // Commit całego batcha
    } catch (Exception e) {
        // Cały batch zostanie przetworzony ponownie
    }
}
```

### 35.4 Offset Storage

Offsets są przechowywane w specjalnym topic: `__consumer_offsets`

```
Topic: __consumer_offsets
Key: (group.id, topic, partition)
Value: offset

Example:
Key: ("eventmaster-projectors", "events.lifecycle", 0)
Value: 1234
```

### 35.5 Seeking to Specific Offset

```java
@Service
@RequiredArgsConstructor
public class OffsetManager {
    
    private final KafkaListenerEndpointRegistry registry;
    
    /**
     * Rewind to beginning
     */
    public void seekToBeginning(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        
        container.stop();
        
        Set<TopicPartition> partitions = getAssignedPartitions(container);
        
        Consumer<?, ?> consumer = getConsumer(container);
        consumer.seekToBeginning(partitions);
        
        container.start();
    }
    
    /**
     * Seek to specific offset
     */
    public void seekToOffset(String listenerId, int partition, long offset) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        
        container.stop();
        
        TopicPartition tp = new TopicPartition("events.lifecycle", partition);
        Consumer<?, ?> consumer = getConsumer(container);
        consumer.seek(tp, offset);
        
        container.start();
    }
    
    /**
     * Seek to timestamp
     */
    public void seekToTimestamp(String listenerId, Instant timestamp) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        
        container.stop();
        
        Set<TopicPartition> partitions = getAssignedPartitions(container);
        
        Map<TopicPartition, Long> timestampMap = partitions.stream()
            .collect(Collectors.toMap(
                tp -> tp,
                tp -> timestamp.toEpochMilli()
            ));
        
        Consumer<?, ?> consumer = getConsumer(container);
        Map<TopicPartition, OffsetAndTimestamp> result = 
            consumer.offsetsForTimes(timestampMap);
        
        result.forEach((tp, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        });
        
        container.start();
    }
}
```

### 35.6 Monitoring Consumer Lag

**Consumer Lag** = różnica między last produced offset a current consumer offset

```
Last produced offset: 10,000
Consumer offset:       9,500
Lag:                     500 messages ⚠️
```

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerLagMonitor {
    
    private final AdminClient adminClient;
    private final MeterRegistry meterRegistry;
    
    @Scheduled(fixedDelay = 60000)  // Every minute
    public void monitorLag() {
        try {
            ListConsumerGroupOffsetsResult result = adminClient
                .listConsumerGroupOffsets("eventmaster-projectors");
            
            Map<TopicPartition, OffsetAndMetadata> offsets = 
                result.partitionsToOffsetAndMetadata().get();
            
            offsets.forEach((tp, offsetMetadata) -> {
                long consumerOffset = offsetMetadata.offset();
                long endOffset = getEndOffset(tp);
                long lag = endOffset - consumerOffset;
                
                meterRegistry.gauge(
                    "kafka.consumer.lag",
                    Tags.of(
                        "topic", tp.topic(),
                        "partition", String.valueOf(tp.partition())
                    ),
                    lag
                );
                
                if (lag > 1000) {
                    log.warn("High consumer lag detected: topic={}, partition={}, lag={}", 
                        tp.topic(), tp.partition(), lag);
                }
            });
            
        } catch (Exception e) {
            log.error("Failed to monitor consumer lag", e);
        }
    }
    
    private long getEndOffset(TopicPartition tp) {
        // Get end offset for partition
        // (implementation depends on Kafka client version)
        return 0; // placeholder
    }
}
```

### 35.7 Testowanie Offset Management

```java
@SpringBootTest
@Testcontainers
class OffsetManagementTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("apache/kafka:7.6.0")
    );
    
    @Autowired
    private KafkaTemplate<String, Event> producer;
    
    @Autowired
    private OffsetManager offsetManager;
    
    @Test
    void shouldCommitOffsetOnlyAfterSuccessfulProcessing() {
        // Given: 10 messages
        for (int i = 0; i < 10; i++) {
            producer.send("events.lifecycle", "key-" + i, createEvent(i));
        }
        
        // When: Consumer processes 5, then crashes
        await().atMost(10, SECONDS).until(() -> processedCount() == 5);
        
        // Simulate crash
        stopConsumer();
        
        // Then: Offset should be at 5
        long offset = getCommittedOffset("eventmaster-projectors", 0);
        assertThat(offset).isEqualTo(5);
        
        // When: Restart consumer
        startConsumer();
        
        // Then: Should resume from offset 5 (process remaining 5)
        await().atMost(10, SECONDS).until(() -> processedCount() == 10);
    }
    
    @Test
    void shouldSeekToBeginning() {
        // Given: 100 messages processed
        sendMessages(100);
        await().until(() -> processedCount() == 100);
        
        // When: Seek to beginning
        offsetManager.seekToBeginning("eventConsumer");
        
        // Then: Should reprocess all 100 messages
        await().until(() -> processedCount() == 200);  // 100 original + 100 reprocessed
    }
}
```

### 35.8 Podsumowanie Tematu 35

**Kluczowe Pojęcia:**
- Offset = position in partition
- Commit = save offset
- Lag = messages behind
- Seek = jump to offset

**Best Practices:**
- Manual commit (reliability)
- Monitor lag
- Handle rebalancing
- Idempotent consumers

**Następny temat:** [Partitioning Strategy](#temat-36-partitioning-strategy)

---

## Temat 36: Partitioning Strategy

### 36.1 Definicja

**Partitioning** = podział messages między partitions na podstawie **klucza** (key).

### 36.2 Default Partitioner

**Kafka default partitioner:**
```
hash(key) % number_of_partitions = partition_number

Example:
Key: "event-123-456"
Hash: 87654321
Partitions: 3
87654321 % 3 = 0 → Partition 0
```

**Właściwość:** Messages z tym samym kluczem ZAWSZE trafiają do tej samej partycji!

### 36.3 Dlaczego Klucz Jest Ważny?

**Scenario: Event lifecycle**

```
Event ID: "abc-123"

1. EventCreatedEvent (key="abc-123") → Partition 0
2. EventPublishedEvent (key="abc-123") → Partition 0  ✅ Ta sama!
3. EventCancelledEvent (key="abc-123") → Partition 0  ✅ Ta sama!

Consumer czyta Partition 0:
1. EventCreated (10:00)
2. EventPublished (11:00)
3. EventCancelled (12:00)

Kolejność ZACHOWANA! ✅
```

**Bez klucza (key=null):**
```
1. EventCreatedEvent → Partition 2
2. EventPublishedEvent → Partition 0
3. EventCancelledEvent → Partition 1

Consumer może dostać w złej kolejności:
- Partition 1: EventCancelled (12:00)
- Partition 0: EventPublished (11:00)
- Partition 2: EventCreated (10:00)

BŁĄD! Cancelled przed Created! 😱
```

### 36.4 Partitioning w EventMaster

```java
@Service
@RequiredArgsConstructor
public class PartitionAwareEventPublisher {
    
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    /**
     * Publish with partition key
     */
    public void publish(DomainEvent event) {
        String topic = getTopicForEvent(event);
        String key = extractPartitionKey(event);
        
        kafkaTemplate.send(topic, key, event);
    }
    
    /**
     * Extract partition key based on event type
     */
    private String extractPartitionKey(DomainEvent event) {
        if (event instanceof EventCreatedEvent e) {
            return e.getEventId().toString();
        } else if (event instanceof EventPublishedEvent e) {
            return e.getEventId().toString();
        } else if (event instanceof BookingCreatedEvent b) {
            return b.getBookingId().toString();
        } else if (event instanceof PaymentProcessedEvent p) {
            return p.getPaymentId().toString();
        }
        
        // Fallback: random partition
        return UUID.randomUUID().toString();
    }
}
```

### 36.5 Custom Partitioner

```java
public class EventPartitioner implements Partitioner {
    
    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster
    ) {
        int partitionCount = cluster.partitionsForTopic(topic).size();
        
        if (key == null) {
            // Random partition
            return ThreadLocalRandom.current().nextInt(partitionCount);
        }
        
        // Custom logic based on key pattern
        String keyStr = key.toString();
        
        if (keyStr.startsWith("event-")) {
            // Events to partition 0
            return 0 % partitionCount;
        } else if (keyStr.startsWith("booking-")) {
            // Bookings to partition 1
            return 1 % partitionCount;
        } else if (keyStr.startsWith("payment-")) {
            // Payments to partition 2
            return 2 % partitionCount;
        }
        
        // Default: hash-based
        return Math.abs(keyStr.hashCode()) % partitionCount;
    }
    
    @Override
    public void close() {}
    
    @Override
    public void configure(Map<String, ?> configs) {}
}
```

**Configuration:**
```java
@Configuration
public class KafkaProducerConfig {
    
    @Bean
    public ProducerFactory<String, DomainEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        // ... other config
        
        // Custom partitioner
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, 
            EventPartitioner.class.getName());
        
        return new DefaultKafkaProducerFactory<>(config);
    }
}
```

### 36.6 Partitioning Strategies

**1. Key-based (ZALECANE dla EventMaster):**
```java
// Same entity ID = same partition = ordering guaranteed
kafkaTemplate.send("events.lifecycle", event.getEventId().toString(), event);
```

**2. Round-robin (key=null):**
```java
// Messages distributed evenly, but NO ordering
kafkaTemplate.send("events.lifecycle", null, event);
```

**3. Custom business logic:**
```java
// Events from same organizer to same partition
String key = event.getOrganizerId().toString();
kafkaTemplate.send("events.lifecycle", key, event);
```

### 36.7 Testowanie Partitioning

```java
@SpringBootTest
@Testcontainers
class PartitioningStrategyTest {
    
    @Autowired
    private EventPublisher publisher;
    
    @Autowired
    private KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    @Test
    void sameKeyShouldGoToSamePartition() {
        // Given
        UUID eventId = UUID.randomUUID();
        String key = eventId.toString();
        
        // When: Send 3 events with same key
        EventCreatedEvent created = new EventCreatedEvent(eventId, "Test", "Warsaw", Instant.now());
        EventPublishedEvent published = new EventPublishedEvent(eventId, Instant.now());
        EventCancelledEvent cancelled = new EventCancelledEvent(eventId, Instant.now());
        
        kafkaTemplate.send("events.lifecycle", key, created);
        kafkaTemplate.send("events.lifecycle", key, published);
        kafkaTemplate.send("events.lifecycle", key, cancelled);
        
        // Then: All 3 should be in same partition
        List<ConsumerRecord<String, DomainEvent>> records = consumeAll();
        
        int partition = records.get(0).partition();
        assertThat(records.get(1).partition()).isEqualTo(partition);
        assertThat(records.get(2).partition()).isEqualTo(partition);
    }
    
    @Test
    void orderingShouldBePreservedWithinPartition() {
        // Given
        UUID eventId = UUID.randomUUID();
        String key = eventId.toString();
        
        // When: Send in order
        Instant t1 = Instant.now();
        Instant t2 = t1.plusSeconds(1);
        Instant t3 = t2.plusSeconds(1);
        
        kafkaTemplate.send("events.lifecycle", key, new EventCreatedEvent(eventId, "Test", "Warsaw", t1));
        Thread.sleep(100);  // Ensure order
        kafkaTemplate.send("events.lifecycle", key, new EventPublishedEvent(eventId, t2));
        Thread.sleep(100);
        kafkaTemplate.send("events.lifecycle", key, new EventCancelledEvent(eventId, t3));
        
        // Then: Consumer receives in same order
        await().atMost(5, SECONDS).untilAsserted(() -> {
            List<DomainEvent> consumed = consumer.getEventsFor(eventId);
            assertThat(consumed).hasSize(3);
            assertThat(consumed.get(0)).isInstanceOf(EventCreatedEvent.class);
            assertThat(consumed.get(1)).isInstanceOf(EventPublishedEvent.class);
            assertThat(consumed.get(2)).isInstanceOf(EventCancelledEvent.class);
        });
    }
}
```

### 36.8 Podsumowanie Tematu 36

**Kluczowe Pojęcia:**
- Partition key = routing decision
- Same key = same partition = ordering
- Hash-based default partitioner
- Custom partitioners możliwe

**Best Practices EventMaster:**
- ✅ Używaj entity ID jako klucza
- ✅ Testuj ordering w partition
- ✅ Monitor partition distribution
- ❌ Nie używaj key=null (except broadcast scenarios)

**Następny temat:** [Error Handling & Dead Letter Queue](#temat-37-error-handling-dead-letter-queue)

---

## Temat 37: Error Handling & Dead Letter Queue

### 37.1 Problem: Co Robić z Nieudanymi Messages?

```
Consumer receives message → Process → EXCEPTION! 😱

Options:
1. Retry infinitely? (Consumer blocked forever)
2. Skip message? (Data loss!)
3. Retry N times, then DLQ? (✅ Best!)
```

### 37.2 Retry Strategy

```java
@Service
@Slf4j
public class EventConsumerWithRetry {
    
    @RetryableTopic(
        attempts = "4",  // 1 initial + 3 retries
        backoff = @Backoff(
            delay = 1000,      // 1 second
            multiplier = 2.0,  // Exponential: 1s, 2s, 4s
            maxDelay = 10000   // Max 10 seconds
        ),
        autoCreateTopics = "true",
        include = {TransientException.class},  // Retry only these
        exclude = {ValidationException.class}, // Don't retry these
        dltTopicSuffix = "-dlt"
    )
    @KafkaListener(topics = "events.lifecycle", groupId = "eventmaster-projectors")
    public void consume(EventCreatedEvent event) {
        log.info("Processing: {}", event.getEventId());
        
        try {
            projector.project(event);
        } catch (DatabaseException e) {
            // Transient error - will retry
            log.warn("Transient error, will retry: {}", e.getMessage());
            throw e;
        } catch (ValidationException e) {
            // Permanent error - no retry, go to DLQ
            log.error("Validation error, sending to DLQ: {}", e.getMessage());
            throw e;
        }
    }
}
```

**Retry Flow:**
```
1. Initial attempt → FAIL
   ↓ wait 1s
2. Retry 1 → FAIL
   ↓ wait 2s
3. Retry 2 → FAIL
   ↓ wait 4s
4. Retry 3 → FAIL
   ↓
5. Send to DLQ (events.lifecycle-dlt)
```

### 37.3 Dead Letter Queue (DLQ)

```java
@Service
@Slf4j
public class DeadLetterQueueHandler {
    
    @KafkaListener(
        topics = "events.lifecycle-dlt",
        groupId = "eventmaster-dlq-processor"
    )
    public void processDLQ(ConsumerRecord<String, EventCreatedEvent> record) {
        log.error("Message in DLQ: topic={}, partition={}, offset={}, key={}", 
            record.topic(), record.partition(), record.offset(), record.key());
        
        // Extract failure reason from headers
        Header reasonHeader = record.headers().lastHeader("kafka_dlt-exception-message");
        String reason = reasonHeader != null ? 
            new String(reasonHeader.value()) : "Unknown";
        
        // Store for manual review
        FailedMessage failed = FailedMessage.builder()
            .originalTopic(record.topic().replace("-dlt", ""))
            .partition(record.partition())
            .offset(record.offset())
            .key(record.key())
            .payload(serializeEvent(record.value()))
            .failureReason(reason)
            .timestamp(Instant.now())
            .status(FailedMessageStatus.PENDING_REVIEW)
            .build();
        
        failedMessageRepository.save(failed);
        
        // Alert ops team
        alertService.notifyFailedMessage(failed);
    }
}
```

### 37.4 DLQ Configuration

```java
@Configuration
public class KafkaRetryConfig {
    
    @Bean
    public RetryTopicConfiguration retryTopicConfiguration(
        KafkaTemplate<String, Object> template
    ) {
        return RetryTopicConfigurationBuilder
            .newInstance()
            .fixedBackOff(1000)  // 1 second between retries
            .maxAttempts(3)
            .concurrency(1)
            .includeTopics("events.lifecycle", "bookings.created")
            .useSingleTopicForSameIntervals()  // events.lifecycle-retry-0, -retry-1...
            .doNotRetryOnDltFailure()
            .dltHandlerMethod("deadLetterQueueHandler", "processDLQ")
            .create(template);
    }
    
    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("events.lifecycle-dlt")
            .partitions(1)
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, 
                String.valueOf(Duration.ofDays(90).toMillis()))
            .build();
    }
}
```

### 37.5 Manual DLQ Replay

```java
@RestController
@RequestMapping("/api/admin/dlq")
@RequiredArgsConstructor
public class DLQAdminController {
    
    private final FailedMessageRepository failedMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    /**
     * List failed messages
     */
    @GetMapping
    public List<FailedMessageDTO> listFailedMessages(
        @RequestParam(defaultValue = "PENDING_REVIEW") FailedMessageStatus status
    ) {
        return failedMessageRepository.findByStatus(status)
            .stream()
            .map(this::toDTO)
            .collect(Collectors.toList());
    }
    
    /**
     * Replay single message
     */
    @PostMapping("/{id}/replay")
    public ResponseEntity<Void> replayMessage(@PathVariable Long id) {
        FailedMessage failed = failedMessageRepository.findById(id)
            .orElseThrow(() -> new EntityNotFoundException("Failed message not found"));
        
        // Deserialize and resend
        DomainEvent event = deserialize(failed.getPayload());
        kafkaTemplate.send(failed.getOriginalTopic(), failed.getKey(), event);
        
        // Mark as replayed
        failed.setStatus(FailedMessageStatus.REPLAYED);
        failed.setReplayedAt(Instant.now());
        failedMessageRepository.save(failed);
        
        return ResponseEntity.ok().build();
    }
    
    /**
     * Replay all pending
     */
    @PostMapping("/replay-all")
    public ResponseEntity<ReplayResult> replayAllPending() {
        List<FailedMessage> pending = failedMessageRepository
            .findByStatus(FailedMessageStatus.PENDING_REVIEW);
        
        int success = 0;
        int failed = 0;
        
        for (FailedMessage msg : pending) {
            try {
                replayMessage(msg.getId());
                success++;
            } catch (Exception e) {
                log.error("Failed to replay message: {}", msg.getId(), e);
                failed++;
            }
        }
        
        return ResponseEntity.ok(new ReplayResult(success, failed));
    }
}
```

### 37.6 Circuit Breaker

```java
@Service
@Slf4j
public class EventConsumerWithCircuitBreaker {
    
    private final CircuitBreaker circuitBreaker;
    
    public EventConsumerWithCircuitBreaker(CircuitBreakerRegistry registry) {
        this.circuitBreaker = registry.circuitBreaker("eventProjector");
    }
    
    @KafkaListener(topics = "events.lifecycle", groupId = "eventmaster-projectors")
    public void consume(EventCreatedEvent event, Acknowledgment ack) {
        try {
            // Wrap in circuit breaker
            circuitBreaker.executeSupplier(() -> {
                projector.project(event);
                return null;
            });
            
            ack.acknowledge();
            
        } catch (CallNotPermittedException e) {
            // Circuit is OPEN - don't process
            log.warn("Circuit breaker OPEN, pausing consumer");
            pauseConsumer();
            
        } catch (Exception e) {
            log.error("Processing failed", e);
            // Will retry via @RetryableTopic
            throw e;
        }
    }
}
```

**Circuit Breaker Configuration:**
```yaml
resilience4j:
  circuitbreaker:
    instances:
      eventProjector:
        slidingWindowSize: 10
        permittedNumberOfCallsInHalfOpenState: 3
        waitDurationInOpenState: 30s
        failureRateThreshold: 50
```

### 37.7 Testowanie Error Handling

```java
@SpringBootTest
@Testcontainers
class ErrorHandlingTest {
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> producer;
    
    @Autowired
    private FailedMessageRepository dlqRepository;
    
    @MockBean
    private EventProjector projector;
    
    @Test
    void shouldRetryTransientErrors() {
        // Given: Projector fails with transient error
        when(projector.project(any()))
            .thenThrow(new DatabaseException("Connection timeout"))
            .thenThrow(new DatabaseException("Connection timeout"))
            .thenReturn(null);  // Success on 3rd attempt
        
        // When: Send event
        EventCreatedEvent event = createEvent();
        producer.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then: Should retry and eventually succeed
        await().atMost(20, SECONDS).untilAsserted(() -> {
            verify(projector, times(3)).project(any());
        });
        
        // DLQ should be empty
        assertThat(dlqRepository.findAll()).isEmpty();
    }
    
    @Test
    void shouldSendToDLQAfterMaxRetries() {
        // Given: Projector always fails
        when(projector.project(any()))
            .thenThrow(new DatabaseException("Permanent failure"));
        
        // When: Send event
        EventCreatedEvent event = createEvent();
        producer.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then: Should retry 3 times, then send to DLQ
        await().atMost(30, SECONDS).untilAsserted(() -> {
            List<FailedMessage> dlqMessages = dlqRepository.findAll();
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).getOriginalTopic()).isEqualTo("events.lifecycle");
            assertThat(dlqMessages.get(0).getFailureReason()).contains("Permanent failure");
        });
    }
    
    @Test
    void shouldNotRetryValidationErrors() {
        // Given: Validation error (non-retryable)
        when(projector.project(any()))
            .thenThrow(new ValidationException("Invalid data"));
        
        // When: Send event
        EventCreatedEvent event = createEvent();
        producer.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then: Should NOT retry, go straight to DLQ
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(projector, times(1)).project(any());  // Only 1 attempt!
            assertThat(dlqRepository.findAll()).hasSize(1);
        });
    }
}
```

### 37.8 Podsumowanie Tematu 37

**Error Handling Strategy:**
1. Retry transient errors (3x with backoff)
2. Don't retry permanent errors
3. Send to DLQ after max retries
4. Manual review and replay

**Best Practices:**
- ✅ Exponential backoff
- ✅ Separate DLQ topic
- ✅ Store failure reason
- ✅ Admin interface for replay
- ✅ Circuit breaker dla protection

**Następny temat:** [Exactly-Once Semantics](#temat-38-exactly-once-semantics)

---

## Temat 38: Exactly-Once Semantics

### 38.1 Delivery Guarantees

**At-Most-Once:**
```
Producer → Kafka → Consumer
Message może zostać zgubiony (not reliable!)
```

**At-Least-Once (domyślnie w Kafka):**
```
Producer → Kafka → Consumer
Message może przyjść 2x lub więcej (duplicates!)
```

**Exactly-Once:**
```
Producer → Kafka → Consumer
Message delivery DOKŁADNIE raz (ideal!)
```

### 38.2 Problem: Duplicates

**Scenario:**
```
1. Consumer receives message
2. Consumer processes message
3. Consumer commits offset
4. CRASH before commit! 😱
5. On restart: Consumer receives SAME message again (duplicate!)
```

### 38.3 Solution 1: Idempotent Consumer (ZALECANE)

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotentEventConsumer {
    
    private final ProcessedEventRepository processedEventRepo;
    private final EventProjector projector;
    
    @KafkaListener(topics = "events.lifecycle", groupId = "eventmaster-projectors")
    @Transactional
    public void consume(EventCreatedEvent event, Acknowledgment ack) {
        UUID eventId = event.getEventId();
        
        // Check if already processed
        if (processedEventRepo.existsByEventId(eventId)) {
            log.info("Event already processed, skipping: {}", eventId);
            ack.acknowledge();
            return;  // Skip duplicate ✅
        }
        
        // Process event
        projector.project(event);
        
        // Mark as processed
        ProcessedEvent processed = ProcessedEvent.builder()
            .eventId(eventId)
            .eventType(event.getClass().getSimpleName())
            .processedAt(Instant.now())
            .build();
        processedEventRepo.save(processed);
        
        // Commit offset
        ack.acknowledge();
    }
}
```

**ProcessedEvent Table:**
```sql
CREATE TABLE processed_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    
    INDEX idx_event_id (event_id)
);
```

### 38.4 Solution 2: Kafka Transactions (Advanced)

```java
@Configuration
public class KafkaTransactionalProducerConfig {
    
    @Bean
    public ProducerFactory<String, DomainEvent> transactionalProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Enable transactions
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eventmaster-tx-producer");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        
        return new DefaultKafkaProducerFactory<>(config);
    }
    
    @Bean
    public KafkaTransactionManager<String, DomainEvent> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }
}
```

```java
@Service
@RequiredArgsConstructor
public class TransactionalEventPublisher {
    
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    private final EventRepository eventRepository;
    
    @Transactional("kafkaTransactionManager")
    public void publishWithTransaction(DomainEvent event) {
        // Kafka transaction начинается automatycznie (@Transactional)
        
        // 1. Send to Kafka (part of transaction)
        kafkaTemplate.send("events.lifecycle", event.getEventId().toString(), event);
        
        // 2. Send another message (part of same transaction)
        kafkaTemplate.send("events.audit", event.getEventId().toString(), event);
        
        // If any fails → both are rolled back! ✅
        // Commit happens automatically at method end
    }
}
```

### 38.5 Exactly-Once with Database (Outbox Pattern - Temat 40)

**Problem:** Kafka transaction NIE obejmuje database!

```java
// ❌ BAD: Database and Kafka not atomic
@Transactional  // Database transaction
public void create(CreateEventCommand cmd) {
    Event event = new Event(cmd);
    eventRepository.save(event);  // DB save
    
    kafkaTemplate.send("events.lifecycle", new EventCreatedEvent(event));  // Kafka send
    // If Kafka fails → Event in DB but no message! 😱
    // If DB fails → Message in Kafka but no event! 😱
}
```

**Solution:** Transactional Outbox Pattern (Temat 40)

### 38.6 Testowanie Idempotency

```java
@SpringBootTest
@Testcontainers
class IdempotencyTest {
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> producer;
    
    @Autowired
    private EventViewRepository eventViewRepo;
    
    @Autowired
    private ProcessedEventRepository processedEventRepo;
    
    @Test
    void consumerShouldBeIdempotent() {
        // Given
        EventCreatedEvent event = createEvent();
        String key = event.getEventId().toString();
        
        // When: Send same message 3 times
        producer.send("events.lifecycle", key, event);
        producer.send("events.lifecycle", key, event);  // Duplicate!
        producer.send("events.lifecycle", key, event);  // Duplicate!
        
        // Then: Only processed once
        await().atMost(10, SECONDS).untilAsserted(() -> {
            // Only 1 event in read model
            List<EventView> views = eventViewRepo.findAll();
            assertThat(views).hasSize(1);
            
            // Only 1 processed record
            List<ProcessedEvent> processed = processedEventRepo.findAll();
            assertThat(processed).hasSize(1);
        });
    }
    
    @Test
    void shouldHandleConcurrentDuplicates() throws Exception {
        // Given
        EventCreatedEvent event = createEvent();
        String key = event.getEventId().toString();
        
        // When: Send same message concurrently (simulate network retry)
        CompletableFuture<Void> future1 = CompletableFuture.runAsync(() ->
            producer.send("events.lifecycle", key, event)
        );
        CompletableFuture<Void> future2 = CompletableFuture.runAsync(() ->
            producer.send("events.lifecycle", key, event)
        );
        
        CompletableFuture.allOf(future1, future2).get();
        
        // Then: Only processed once (database constraint prevents duplicates)
        await().atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(eventViewRepo.count()).isEqualTo(1);
        });
    }
}
```

### 38.7 Podsumowanie Tematu 38

**Delivery Guarantees:**
- At-Most-Once = możliwa utrata
- At-Least-Once = możliwe duplikaty (domyślne)
- Exactly-Once = ideał (trudne!)

**Solutions:**
- ✅ Idempotent Consumer (najprostsze, ZALECANE)
- ✅ Kafka Transactions (advanced)
- ✅ Transactional Outbox (database + Kafka atomicity)

**Best Practice:** Zawsze rób consumers idempotent!

**Następny temat:** [Schema Registry & Event Versioning](#temat-39-schema-registry-event-versioning)

---

## Temat 39: Schema Registry & Event Versioning

### 39.1 Problem: Event Schema Changes

```java
// Version 1 (deployed 6 months ago)
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String location  // "Warsaw, Poland"
) {}

// Version 2 (new requirement: split location)
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String city,     // "Warsaw"
    String country   // "Poland"
) {}

// Problem: Old messages in Kafka use V1 schema!
// Consumers expecting V2 will FAIL! 😱
```

### 39.2 Schema Registry

**Schema Registry** = centralized service storing event schemas.

**Architecture:**
```
Producer → Check schema → Schema Registry
    ↓                           ↓
Send with schema ID      Store/validate schema
    ↓
Kafka (message + schema ID)
    ↓
Consumer → Fetch schema ← Schema Registry
    ↓
Deserialize with correct schema
```

### 39.3 Avro Schema Example

```json
{
  "type": "record",
  "name": "EventCreatedEvent",
  "namespace": "com.eventmaster.events",
  "fields": [
    {"name": "eventId", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "location", "type": "string"},
    {
      "name": "city", 
      "type": ["null", "string"], 
      "default": null
    },
    {
      "name": "country", 
      "type": ["null", "string"], 
      "default": null
    }
  ]
}
```

### 39.4 Schema Evolution Rules

**FORWARD compatible (nowy consumer, stary producer):**
```
✅ Add optional field (with default)
✅ Remove field
❌ Add required field
❌ Change field type
```

**BACKWARD compatible (stary consumer, nowy producer):**
```
✅ Add field (consumer ignores)
✅ Remove optional field
❌ Remove required field
❌ Change field type
```

**FULL compatible (best!):**
```
✅ Add optional field with default
✅ Remove optional field
```

### 39.5 EventMaster Strategy: Event Versioning with JSON

Nie używamy Schema Registry (complexity), używamy **event versioning w JSON**:

```java
// Version 1
public record EventCreatedEventV1(
    UUID eventId,
    String name,
    String location,
    int version  // = 1
) implements DomainEvent {}

// Version 2
public record EventCreatedEventV2(
    UUID eventId,
    String name,
    String city,
    String country,
    int version  // = 2
) implements DomainEvent {}
```

**Deserializer with versioning:**
```java
@Component
public class VersionedEventDeserializer {
    
    private final ObjectMapper objectMapper;
    
    public DomainEvent deserialize(String json) {
        JsonNode node = objectMapper.readTree(json);
        
        String eventType = node.get("type").asText();
        int version = node.has("version") ? node.get("version").asInt() : 1;
        
        // Route to correct version
        if ("EventCreatedEvent".equals(eventType)) {
            if (version == 1) {
                return objectMapper.treeToValue(node, EventCreatedEventV1.class);
            } else if (version == 2) {
                return objectMapper.treeToValue(node, EventCreatedEventV2.class);
            }
        }
        
        throw new DeserializationException("Unknown event type/version: " + eventType + " v" + version);
    }
}
```

**Upcaster:**
```java
@Component
public class EventCreatedEventUpcaster {
    
    public EventCreatedEventV2 upcast(EventCreatedEventV1 v1) {
        // Parse location "City, Country"
        String[] parts = v1.location().split(",\\s*");
        String city = parts.length > 0 ? parts[0] : "";
        String country = parts.length > 1 ? parts[1] : "";
        
        return new EventCreatedEventV2(
            v1.eventId(),
            v1.name(),
            city,
            country,
            2  // New version
        );
    }
}
```

### 39.6 Testowanie Event Versioning

```java
@SpringBootTest
class EventVersioningTest {
    
    @Autowired
    private VersionedEventDeserializer deserializer;
    
    @Autowired
    private EventCreatedEventUpcaster upcaster;
    
    @Test
    void shouldDeserializeV1Event() {
        // Given: V1 event JSON
        String json = """
            {
                "type": "EventCreatedEvent",
                "version": 1,
                "eventId": "123e4567-e89b-12d3-a456-426614174000",
                "name": "JavaConf",
                "location": "Warsaw, Poland"
            }
            """;
        
        // When
        DomainEvent event = deserializer.deserialize(json);
        
        // Then
        assertThat(event).isInstanceOf(EventCreatedEventV1.class);
        EventCreatedEventV1 v1 = (EventCreatedEventV1) event;
        assertThat(v1.location()).isEqualTo("Warsaw, Poland");
    }
    
    @Test
    void shouldUpcastV1ToV2() {
        // Given
        EventCreatedEventV1 v1 = new EventCreatedEventV1(
            UUID.randomUUID(),
            "JavaConf",
            "Berlin, Germany",
            1
        );
        
        // When
        EventCreatedEventV2 v2 = upcaster.upcast(v1);
        
        // Then
        assertThat(v2.version()).isEqualTo(2);
        assertThat(v2.city()).isEqualTo("Berlin");
        assertThat(v2.country()).isEqualTo("Germany");
    }
}
```

### 39.7 Podsumowanie Tematu 39

**Schema Evolution:**
- Events change over time
- Old messages exist in Kafka
- Need backward/forward compatibility

**Solutions:**
- Schema Registry (Avro/Protobuf) - enterprise
- Event Versioning (JSON) - simpler (EventMaster)
- Upcasting - V1 → V2 transformation

**Best Practices:**
- Version field in all events
- Additive changes only
- Test deserialization of old events

**Następny temat:** [Transactional Outbox Pattern](#temat-40-transactional-outbox-pattern)

---

## Temat 40: Transactional Outbox Pattern

### 40.1 Problem: Database + Kafka Atomicity

```java
// ❌ PROBLEM: Not atomic!
@Transactional
public void createEvent(CreateEventCommand cmd) {
    // 1. Save to database
    Event event = new Event(cmd);
    eventRepository.save(event);  // Transaction 1 (DB)
    
    // 2. Publish to Kafka
    kafkaTemplate.send("events.lifecycle", new EventCreatedEvent(event));  // Transaction 2 (Kafka)
    
    // Possible failures:
    // - DB saves, Kafka fails → Event in DB, no message 😱
    // - DB fails, Kafka succeeds → Message in Kafka, no event 😱
}
```

### 40.2 Solution: Transactional Outbox

**Idea:** Zapisz message do database w tej samej transakcji, potem asynchronicznie wyślij do Kafka.

```
┌──────────────────────────────────────┐
│        Database Transaction          │
│  ┌────────────┐  ┌────────────────┐ │
│  │ Event Table│  │ Outbox Table   │ │
│  │  - id      │  │  - id          │ │
│  │  - name    │  │  - event_id    │ │
│  │  - status  │  │  - payload     │ │
│  └────────────┘  │  - published   │ │
│                  └────────────────┘ │
│   Both saved ATOMICALLY! ✅         │
└──────────────────────────────────────┘
          ↓
    Background Job
          ↓
  ┌───────────────┐
  │  Kafka        │
  │  - events.    │
  │    lifecycle  │
  └───────────────┘
```

### 40.3 Outbox Table Schema

```sql
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    
    event_type VARCHAR(200) NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,
    
    topic VARCHAR(200) NOT NULL,
    partition_key VARCHAR(500),
    
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    
    INDEX idx_unpublished (published_at) WHERE published_at IS NULL
);
```

### 40.4 Implementation

```java
@Entity
@Table(name = "outbox_events")
@Data
@NoArgsConstructor
public class OutboxEvent {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private UUID aggregateId;
    
    @Column(nullable = false)
    private String aggregateType;
    
    @Column(nullable = false)
    private String eventType;
    
    @Column(nullable = false, unique = true)
    private UUID eventId;
    
    @Type(JsonBinaryType.class)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;
    
    @Column(nullable = false)
    private Instant createdAt;
    
    @Column
    private Instant publishedAt;
    
    @Column(nullable = false)
    private String topic;
    
    @Column
    private String partitionKey;
    
    @Column(nullable = false)
    private Integer retryCount = 0;
    
    @Column
    private String errorMessage;
    
    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (eventId == null) {
            eventId = UUID.randomUUID();
        }
    }
    
    public boolean isPublished() {
        return publishedAt != null;
    }
}
```

**Service:**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventService {
    
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    
    /**
     * Save event to outbox (same transaction as business logic)
     */
    @Transactional
    public void saveToOutbox(DomainEvent event, String topic, String partitionKey) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            
            OutboxEvent outbox = new OutboxEvent();
            outbox.setAggregateId(extractAggregateId(event));
            outbox.setAggregateType(extractAggregateType(event));
            outbox.setEventType(event.getClass().getSimpleName());
            outbox.setEventId(event.getEventId());
            outbox.setPayload(payload);
            outbox.setTopic(topic);
            outbox.setPartitionKey(partitionKey);
            
            outboxRepository.save(outbox);
            
            log.debug("Event saved to outbox: eventId={}", event.getEventId());
            
        } catch (Exception e) {
            log.error("Failed to save event to outbox", e);
            throw new OutboxException("Failed to save event to outbox", e);
        }
    }
}
```

**Publisher (Background Job):**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventPublisher {
    
    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    @Scheduled(fixedDelay = 1000)  // Every second
    @Transactional
    public void publishPendingEvents() {
        // Find unpublished events (limit 100)
        List<OutboxEvent> pending = outboxRepository
            .findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
        
        if (pending.isEmpty()) {
            return;
        }
        
        log.debug("Publishing {} outbox events", pending.size());
        
        for (OutboxEvent outbox : pending) {
            try {
                // Publish to Kafka
                kafkaTemplate.send(
                    outbox.getTopic(),
                    outbox.getPartitionKey(),
                    outbox.getPayload()
                ).get(5, TimeUnit.SECONDS);  // Wait for confirmation
                
                // Mark as published
                outbox.setPublishedAt(Instant.now());
                outboxRepository.save(outbox);
                
                log.debug("Published event: eventId={}", outbox.getEventId());
                
            } catch (Exception e) {
                log.error("Failed to publish event: eventId={}", outbox.getEventId(), e);
                
                // Increment retry count
                outbox.setRetryCount(outbox.getRetryCount() + 1);
                outbox.setErrorMessage(e.getMessage());
                outboxRepository.save(outbox);
                
                // If too many retries, alert
                if (outbox.getRetryCount() > 10) {
                    log.error("Event failed after 10 retries: eventId={}", outbox.getEventId());
                    // Alert ops team
                }
            }
        }
    }
}
```

### 40.5 Using Outbox in Command Handler

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class CreateEventCommandHandler {
    
    private final EventRepository eventRepository;
    private final OutboxEventService outboxService;
    
    @Transactional
    public void handle(CreateEventCommand command) {
        log.info("Creating event: {}", command.getName());
        
        // 1. Business logic - save to database
        Event event = Event.create(
            UUID.randomUUID(),
            command.getName(),
            command.getDescription(),
            command.getLocation(),
            command.getStartDate()
        );
        
        eventRepository.save(event);
        
        // 2. Save event to outbox (same transaction!)
        EventCreatedEvent domainEvent = new EventCreatedEvent(
            event.getId(),
            event.getName(),
            event.getLocation(),
            Instant.now()
        );
        
        outboxService.saveToOutbox(
            domainEvent,
            "events.lifecycle",
            event.getId().toString()
        );
        
        log.info("Event created and saved to outbox: {}", event.getId());
        
        // If ANYTHING fails above → BOTH are rolled back! ✅
    }
}
```

### 40.6 Cleanup Old Outbox Events

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventCleanup {
    
    private final OutboxEventRepository outboxRepository;
    
    @Scheduled(cron = "0 0 2 * * *")  // 2 AM daily
    @Transactional
    public void cleanupOldEvents() {
        Instant cutoff = Instant.now().minus(7, ChronoUnit.DAYS);
        
        int deleted = outboxRepository.deleteByPublishedAtIsNotNullAndPublishedAtBefore(cutoff);
        
        log.info("Cleaned up {} old outbox events", deleted);
    }
}
```

### 40.7 Testowanie Outbox Pattern

```java
@SpringBootTest
@Testcontainers
class TransactionalOutboxTest {
    
    @Autowired
    private CreateEventCommandHandler handler;
    
    @Autowired
    private EventRepository eventRepository;
    
    @Autowired
    private OutboxEventRepository outboxRepository;
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Test
    void shouldSaveEventAndOutboxAtomically() {
        // Given
        CreateEventCommand command = new CreateEventCommand(
            "JavaConf",
            "Java conference",
            "Warsaw",
            LocalDateTime.now().plusMonths(1)
        );
        
        // When
        handler.handle(command);
        
        // Then: Both Event and Outbox saved
        List<Event> events = eventRepository.findAll();
        assertThat(events).hasSize(1);
        
        List<OutboxEvent> outboxEvents = outboxRepository.findAll();
        assertThat(outboxEvents).hasSize(1);
        assertThat(outboxEvents.get(0).getEventType()).isEqualTo("EventCreatedEvent");
        assertThat(outboxEvents.get(0).isPublished()).isFalse();
    }
    
    @Test
    void shouldRollbackBothOnFailure() {
        // Given: Outbox service throws exception
        doThrow(new RuntimeException("Outbox error"))
            .when(outboxService).saveToOutbox(any(), any(), any());
        
        // When / Then
        assertThatThrownBy(() -> handler.handle(command))
            .isInstanceOf(RuntimeException.class);
        
        // Both should be rolled back
        assertThat(eventRepository.findAll()).isEmpty();
        assertThat(outboxRepository.findAll()).isEmpty();
    }
    
    @Test
    void shouldPublishOutboxEventsToKafka() {
        // Given: Event in outbox
        handler.handle(command);
        
        // When: Publisher runs
        outboxPublisher.publishPendingEvents();
        
        // Then: Event published to Kafka
        await().atMost(10, SECONDS).untilAsserted(() -> {
            OutboxEvent outbox = outboxRepository.findAll().get(0);
            assertThat(outbox.isPublished()).isTrue();
            assertThat(outbox.getPublishedAt()).isNotNull();
        });
    }
}
```

### 40.8 Podsumowanie Tematu 40

**Problem:** Database + Kafka nie są atomic

**Solution:** Transactional Outbox Pattern
1. Save to DB + Outbox (atomic)
2. Background job publishes to Kafka
3. Guarantees: at-least-once delivery

**Best Practices:**
- ✅ Outbox w tej samej transakcji
- ✅ Background publisher (scheduled)
- ✅ Retry logic
- ✅ Cleanup old events
- ✅ Monitor failed events

---

## 🎓 Podsumowanie CZĘŚĆ IV

### ✅ Ukończone Tematy (31-40):

31. **Apache Kafka - Wprowadzenie** - distributed log, KRaft mode, Docker Compose
32. **Topics & Partitions** - partitioning for scalability, retention policies
33. **Producers** - KafkaTemplate, serialization, confirmation, headers
34. **Consumers & Consumer Groups** - load balancing, manual commit, rebalancing
35. **Offset Management** - commit strategies, seeking, lag monitoring
36. **Partitioning Strategy** - key-based routing, ordering guarantees
37. **Error Handling & DLQ** - retry, exponential backoff, dead letter queue
38. **Exactly-Once Semantics** - idempotent consumers, Kafka transactions
39. **Schema Registry** - event versioning, upcasting, compatibility
40. **Transactional Outbox** - database + Kafka atomicity

### 🎯 Zdobyte Umiejętności:

- ✅ Konfiguracja Apache Kafka 7.6 (KRaft mode)
- ✅ Implementacja Producers i Consumers
- ✅ Partitioning strategies dla ordering
- ✅ Error handling (retry + DLQ)
- ✅ Exactly-Once przez idempotency
- ✅ Event versioning i upcasting
- ✅ Transactional Outbox dla atomicity

### 📊 Statystyki CZĘŚĆ IV:

```
Tematy:                        10
Linie kodu przykładowego:    200+
Docker Compose config:         1
Strategie error handling:      5
Delivery guarantees:           3
```

### 🛠️ Stack Technologiczny:

- **Apache Kafka 7.6** - KRaft mode (bez Zookeeper)
- **Spring Kafka** - KafkaTemplate, @KafkaListener
- **PostgreSQL 18** - Outbox table
- **Testcontainers** - KafkaContainer
- **Awaitility** - Async testing

### 🚀 Kafka Best Practices:

**Configuration:**
- acks=all (reliability)
- enable-idempotence=true
- manual commit
- 3-6 partitions start

**Partitioning:**
- Use entity ID as key
- Same key = same partition = ordering
- Test partition distribution

**Error Handling:**
- Retry transient errors (3x backoff)
- DLQ for permanent errors
- Manual review and replay

**Reliability:**
- Idempotent consumers (MUST!)
- Transactional Outbox
- Monitor lag
- Circuit breaker

### 🔥 Performance Tips:

```
Throughput optimization:
- Batch sending (linger.ms=10)
- Compression (snappy)
- Multiple partitions
- Consumer groups

Latency optimization:
- acks=1 (instead of all)
- linger.ms=0
- Fewer partitions
```

### 🎓 Następne Kroki (opcjonalne):

**Dla Projektu EventMaster:**
1. Zaimplementuj wszystkie 40 tematów
2. Dodaj monitoring (Prometheus + Grafana)
3. Load testing z Gatling
4. Deployment na Kubernetes

**Dla Dalszej Nauki:**
- CZĘŚĆ V: Testowanie & Operacje (Tematy 41-50)
- Kafka Streams
- ksqlDB
- Kafka Connect

---

## 📚 Dodatkowe Zasoby

### Książki:
1. **"Kafka: The Definitive Guide"** - Neha Narkhede et al.
2. **"Building Event-Driven Microservices"** - Adam Bellemare
3. **"Designing Data-Intensive Applications"** - Martin Kleppmann

### Dokumentacja:
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/reference/)
- [Confluent Developer](https://developer.confluent.io/)

---

**Dokument ukończony:** 2025-01-20  
**Wersja:** 1.0  
**Status:** ✅ KOMPLETNA  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4  
**Autorzy:** EventMaster Architecture Team

---

**🎉 Gratulacje! Ukończyłeś CZĘŚĆ IV: Messaging & Kafka! 🎉**

**Teraz znasz:**
- ✅ Apache Kafka architecture
- ✅ Producers & Consumers
- ✅ Partitioning strategies
- ✅ Error handling (retry + DLQ)
- ✅ Exactly-Once Semantics
- ✅ Event versioning
- ✅ Transactional Outbox

**🚀 Gotowy do implementacji messaging w EventMaster! 🚀**

