# 🧪 Testing Matrix - EventMaster

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Apache Kafka + Spring Boot 3.4 + Nuxt 3  
**Cel:** Kompletna macierz scenariuszy testowych dla systemów event-driven  
**Perspektywa:** QA Engineer / Test Automation Engineer

---

## 📋 Spis Treści

1. [Event Processing Matrix](#1-event-processing-matrix)
2. [Domain vs Integration Events](#2-domain-vs-integration-events-testing)
3. [Eventual Consistency SLA Matrix](#3-eventual-consistency-sla-matrix)
4. [Kafka Resilience Testing](#4-kafka-resilience-testing)
5. [Outbox Pattern Test Scenarios](#5-outbox-pattern-test-scenarios)
6. [Dead Letter Queue (DLQ) Validation](#6-dead-letter-queue-validation)
7. [Security Testing Matrix](#7-security-testing-matrix)
8. [Performance Benchmarks](#8-performance-benchmarks)
9. [Chaos Engineering Scenarios](#9-chaos-engineering-scenarios)
10. [Contract Testing Matrix](#10-contract-testing-matrix)

---

## 1. Event Processing Matrix

Macierz testowa dla różnych scenariuszy przetwarzania eventów.

### 1.1 Podstawowa Matryca

| Event Type | Duplikat | Reorder | Brak Projekcji | Versioning | Upcast Fail | DLQ | Status |
|------------|----------|---------|----------------|------------|-------------|-----|--------|
| **EventCreated** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 100% |
| **EventPublished** | ✅ | ✅ | ✅ | ✅ | ⏳ | ✅ | 83% |
| **EventCancelled** | ✅ | ✅ | ⏳ | ⏳ | ⏳ | ✅ | 50% |
| **TicketPurchased** | ✅ | ✅ | ✅ | ⏳ | ⏳ | ✅ | 67% |
| **PaymentProcessed** | ✅ | ⏳ | ⏳ | ⏳ | ⏳ | ✅ | 33% |
| **UserRegistered** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 100% |

**Legenda:**
- ✅ = Test zaimplementowany i przechodzi
- ⏳ = Do zaimplementowania
- ❌ = Test nie przeszedł / bug
- 🚧 = W trakcie implementacji

### 1.2 Test Duplikatów (Idempotency)

**Cel:** Weryfikacja, że przetworzenie tego samego eventu wielokrotnie nie powoduje duplikatów w projekcji.

**Przykładowy test:**
```java
@Test
void shouldHandleDuplicateEventCreatedIdempotently() {
    // Given: Event z unikalnym eventId
    UUID eventId = UUID.randomUUID();
    EventCreatedEvent event = new EventCreatedEvent(
        eventId,
        "Spring Conference",
        LocalDate.of(2025, 6, 15)
    );
    
    // When: Publikuj ten sam event 3 razy (symulacja at-least-once)
    kafkaProducer.send("events.created", eventId.toString(), event);
    kafkaProducer.send("events.created", eventId.toString(), event); // duplikat
    kafkaProducer.send("events.created", eventId.toString(), event); // duplikat
    
    // Then: Event w projekcji tylko raz
    await().atMost(10, SECONDS).untilAsserted(() -> {
        EventProjection projection = repository.findById(eventId).orElseThrow();
        assertThat(projection.getName()).isEqualTo("Spring Conference");
    });
    
    // And: Brak duplikatów w bazie
    long count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM events WHERE id = ?", 
        Long.class, 
        eventId
    );
    assertThat(count).isEqualTo(1);
    
    // And: ProcessedEvent table zawiera deduplikację
    long processedCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM processed_events WHERE event_id = ?",
        Long.class,
        eventId
    );
    assertThat(processedCount).isEqualTo(3); // Wszystkie 3 prób zapisane
}
```

**Kryteria akceptacji:**
- Event pojawia się w projekcji dokładnie raz
- `processed_events` table loguje wszystkie próby
- Performance: < 100ms dodatkowego opóźnienia dla deduplikacji

### 1.3 Test Reorderingu

**Cel:** Weryfikacja poprawnego obsługiwania eventów dostarczonych poza kolejnością.

**Przykładowy test:**
```java
@Test
void shouldHandleOutOfOrderEvents() {
    // Given: Sekwencja eventów
    UUID eventId = UUID.randomUUID();
    EventCreatedEvent created = new EventCreatedEvent(eventId, "Conference", ...);
    EventPublishedEvent published = new EventPublishedEvent(eventId);
    EventCancelledEvent cancelled = new EventCancelledEvent(eventId);
    
    // When: Publikuj poza kolejnością (3, 1, 2)
    kafkaProducer.send(cancelled);  // Sequence 3 (first!)
    kafkaProducer.send(created);    // Sequence 1
    kafkaProducer.send(published);  // Sequence 2
    
    // Then: Aggregate w poprawnym stanie (CANCELLED)
    await().atMost(10, SECONDS).untilAsserted(() -> {
        EventAggregate aggregate = aggregateRepository.load(eventId);
        assertThat(aggregate.getStatus()).isEqualTo(EventStatus.CANCELLED);
    });
    
    // And: Event store zawiera wszystkie eventy w kolejności sekwencji
    List<StoredEvent> events = eventStore.loadEvents(eventId);
    assertThat(events).hasSize(3);
    assertThat(events.get(0).getType()).isEqualTo("EventCreated");
    assertThat(events.get(1).getType()).isEqualTo("EventPublished");
    assertThat(events.get(2).getType()).isEqualTo("EventCancelled");
}
```

**Implementacja - Version Field:**
```java
@Entity
public class StoredEvent {
    @Id
    private UUID id;
    
    private UUID aggregateId;
    
    @Column(nullable = false)
    private Long version;  // Sekwencja eventów
    
    private String eventType;
    private String payload;
    private Instant timestamp;
    
    // Constraint: UNIQUE(aggregateId, version)
}
```

### 1.4 Test Braku Projekcji

**Cel:** Weryfikacja, że event nie zostanie utracony jeśli projekcja nie istnieje.

**Przykładowy test:**
```java
@Test
void shouldBufferEventWhenProjectionNotReady() {
    // Given: Event processor wyłączony (projekcja nie działa)
    projectionService.stop();
    
    // When: Publikuj event
    UUID eventId = UUID.randomUUID();
    EventCreatedEvent event = new EventCreatedEvent(eventId, ...);
    kafkaProducer.send("events.created", event);
    
    // Then: Event pozostaje w Kafka (offset nie zacommitowany)
    await().during(5, SECONDS).atMost(6, SECONDS).untilAsserted(() -> {
        assertThat(repository.findById(eventId)).isEmpty();
    });
    
    // When: Uruchom projekcję ponownie
    projectionService.start();
    
    // Then: Event przetworzony
    await().atMost(10, SECONDS).untilAsserted(() -> {
        assertThat(repository.findById(eventId)).isPresent();
    });
}
```

### 1.5 Test Versioning (Schema Evolution)

**Cel:** Weryfikacja, że nowa wersja aplikacji poprawnie odczytuje stare eventy.

**Przykładowy test:**
```java
@Test
void shouldReadLegacyEventV1WithNewConsumer() {
    // Given: Event w starej wersji (V1) - brak pola "location"
    String eventV1Json = """
        {
          "eventId": "123e4567-e89b-12d3-a456-426614174000",
          "name": "Spring Conference",
          "date": "2025-06-15"
        }
        """;
    
    // When: Nowa wersja aplikacji (V2) odbiera event
    EventCreatedEventV2 event = objectMapper.readValue(eventV1Json, EventCreatedEventV2.class);
    
    // Then: Pole "location" ma domyślną wartość
    assertThat(event.getLocation()).isEqualTo("TBD");
    
    // And: Event przetworzony poprawnie
    projectionService.handle(event);
    EventProjection projection = repository.findById(event.getEventId()).orElseThrow();
    assertThat(projection.getLocation()).isEqualTo("TBD");
}
```

**Schema V1 vs V2:**
```java
// V1 (stara wersja)
public class EventCreatedEventV1 {
    private UUID eventId;
    private String name;
    private LocalDate date;
}

// V2 (nowa wersja z backward compatibility)
public class EventCreatedEventV2 {
    private UUID eventId;
    private String name;
    private LocalDate date;
    
    @JsonProperty(defaultValue = "TBD")
    private String location = "TBD";  // Nowe pole z default
}
```

### 1.6 Test Upcast Failure

**Cel:** Weryfikacja obsługi błędów podczas upcasting starych eventów.

**Przykładowy test:**
```java
@Test
void shouldSendToD LQWhenUpcastFails() {
    // Given: Event V1 z niepoprawnym formatem daty
    String corruptedEventJson = """
        {
          "eventId": "123",
          "name": "Conference",
          "date": "INVALID_DATE"
        }
        """;
    
    // When: Upcast job próbuje zmigrowć
    kafkaProducer.send("events.created.v1", corruptedEventJson);
    
    // Then: Event trafia do DLQ
    await().atMost(10, SECONDS).untilAsserted(() -> {
        ConsumerRecords<String, String> dlqRecords = dlqConsumer.poll(Duration.ofSeconds(1));
        assertThat(dlqRecords).hasSize(1);
        
        ConsumerRecord<String, String> dlqRecord = dlqRecords.iterator().next();
        assertThat(dlqRecord.value()).contains("INVALID_DATE");
        assertThat(dlqRecord.headers())
            .contains(
                new RecordHeader("dlq.error.type", "UpcastException".getBytes()),
                new RecordHeader("dlq.original.topic", "events.created.v1".getBytes())
            );
    });
}
```

---

## 2. Domain vs Integration Events Testing

Matryca różnic testowych między Domain Events (wewnątrz bounded context) a Integration Events (między serwisami).

### 2.1 Porównanie

| Aspekt | Domain Event | Integration Event |
|--------|--------------|-------------------|
| **Scope** | Wewnątrz BC | Między serwisami |
| **Schema** | Może się zmieniać swobodnie | Wersjonowany (strict) |
| **Testowanie** | Unit + Integration | Contract Tests (Pact) |
| **Idempotency** | Opcjonalna | **Wymagana** |
| **Retry** | W ramach transakcji | Zewnętrzne (Kafka retry) |
| **Przykład** | `EventPublished` | `EventPublicationNotified` |
| **Konsumenci** | 1 serwis | Wielu serwisów |
| **Breaking change** | OK (internal) | ❌ Blokuje deployment |

### 2.2 Test Domain Event (Internal)

**Przykład - EventPublished:**
```java
@Test
void shouldHandleDomainEventPublished() {
    // Given: Event wewnątrz Event Management Context
    UUID eventId = UUID.randomUUID();
    
    // When: Command publikuje event
    PublishEventCommand command = new PublishEventCommand(eventId);
    commandBus.send(command);
    
    // Then: Domain event emitowany
    await().untilAsserted(() -> {
        EventAggregate aggregate = aggregateRepository.load(eventId);
        assertThat(aggregate.getStatus()).isEqualTo(EventStatus.PUBLISHED);
        
        // Projection zaktualizowana
        EventProjection projection = readRepository.findById(eventId).orElseThrow();
        assertThat(projection.getPublishedAt()).isNotNull();
    });
    
    // No need for strict schema validation (internal event)
}
```

### 2.3 Test Integration Event (External)

**Przykład - EventPublicationNotified (do Analytics Service):**
```java
@Test
@PactVerification(value = "analytics-service", pactMethod = "eventPublicationNotified")
void shouldPublishIntegrationEventWithStrictSchema() {
    // Given: Event opublikowany
    UUID eventId = UUID.randomUUID();
    PublishEventCommand command = new PublishEventCommand(eventId);
    commandBus.send(command);
    
    // Then: Integration event wysłany do Kafka
    await().atMost(10, SECONDS).untilAsserted(() -> {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
        assertThat(records).hasSize(1);
        
        String payload = records.iterator().next().value();
        
        // Strict schema validation (contract)
        EventPublicationNotifiedEvent event = objectMapper.readValue(
            payload, 
            EventPublicationNotifiedEvent.class
        );
        
        // Wszystkie wymagane pola obecne
        assertThat(event.getEventId()).isNotNull();
        assertThat(event.getName()).isNotEmpty();
        assertThat(event.getPublishedAt()).isNotNull();
        assertThat(event.getEventType()).isEqualTo("CONFERENCE");
        
        // JSON Schema validation
        JsonSchema schema = loadSchemaFromRegistry("EventPublicationNotified-v1");
        assertThat(schema.validate(payload)).isEmpty(); // No violations
    });
}
```

**Kontrakt Pact (dla konsumenta):**
```java
@Pact(consumer = "analytics-service", provider = "event-service")
public RequestResponsePact eventPublicationNotified(PactDslWithProvider builder) {
    return builder
        .given("event exists and is published")
        .uponReceiving("EventPublicationNotified message")
        .method("POST")
        .path("/events/published")
        .body(newJsonBody((body) -> {
            body.uuid("eventId");
            body.stringType("name", "Spring Conference");
            body.datetime("publishedAt", "yyyy-MM-dd'T'HH:mm:ss'Z'");
            body.stringValue("eventType", "CONFERENCE");
        }).build())
        .willRespondWith()
        .status(202)
        .toPact();
}
```

---

## 3. Eventual Consistency SLA Matrix

Macierz opóźnień dla różnych typów eventów z kryteriami akceptacji SLA.

### 3.1 Metryki Docelowe

| Event Type | p50 | p95 | p99 | p99.9 | SLA Threshold | Criticality |
|------------|-----|-----|-----|-------|---------------|-------------|
| **EventCreated** | 50ms | 150ms | 300ms | 1s | < 500ms (p99) | MEDIUM |
| **EventPublished** | 80ms | 200ms | 400ms | 1.5s | < 600ms (p99) | MEDIUM |
| **TicketPurchased** | 100ms | 500ms | 2s | 5s | < 3s (p99) | **HIGH** |
| **PaymentProcessed** | 200ms | 1s | 3s | 10s | < 5s (p99) | **CRITICAL** |
| **UserRegistered** | 150ms | 500ms | 1s | 3s | < 2s (p99) | MEDIUM |
| **EmailSent** | 500ms | 2s | 5s | 15s | < 10s (p99) | LOW |

**Legenda Criticality:**
- **CRITICAL** - Wpływa bezpośrednio na UX, pieniądze
- **HIGH** - Ważne dla biznesu
- **MEDIUM** - Standardowe operacje
- **LOW** - Procesy w tle

### 3.2 Test Eventual Consistency SLA

**Przykładowy test (TicketPurchased):**
```java
@Test
void shouldMeetEventualConsistencySLAForTicketPurchase() {
    // Given: 1000 zakupów biletów
    List<Duration> latencies = new ArrayList<>();
    
    for (int i = 0; i < 1000; i++) {
        UUID ticketId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        
        long startNanos = System.nanoTime();
        
        // When: Publish command
        PurchaseTicketCommand command = new PurchaseTicketCommand(ticketId, eventId, "John Doe");
        commandBus.send(command);
        
        // And: Wait for projection update
        await().atMost(30, SECONDS).until(() -> 
            ticketRepository.findById(ticketId).isPresent()
        );
        
        long endNanos = System.nanoTime();
        latencies.add(Duration.ofNanos(endNanos - startNanos));
    }
    
    // Then: Calculate percentiles
    Duration p50 = calculatePercentile(latencies, 50);
    Duration p95 = calculatePercentile(latencies, 95);
    Duration p99 = calculatePercentile(latencies, 99);
    Duration p999 = calculatePercentile(latencies, 99.9);
    
    // Assert SLA
    assertThat(p99).isLessThan(Duration.ofSeconds(3)); // < 3s (p99)
    
    // Log metrics
    log.info("TicketPurchased latency: p50={}, p95={}, p99={}, p99.9={}", 
        p50, p95, p99, p999);
}

private Duration calculatePercentile(List<Duration> latencies, double percentile) {
    List<Duration> sorted = latencies.stream()
        .sorted()
        .toList();
    int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
    return sorted.get(index);
}
```

### 3.3 Monitoring - Prometheus Metrics

**Eksponowane metryki:**
```java
@Component
public class EventProcessingMetrics {
    
    private final Timer eventProcessingTimer;
    private final Counter eventProcessedCounter;
    
    public EventProcessingMetrics(MeterRegistry registry) {
        this.eventProcessingTimer = Timer.builder("event.processing.duration")
            .tag("event_type", "EventCreated")
            .description("Time taken to process event and update projection")
            .publishPercentiles(0.5, 0.95, 0.99, 0.999)
            .register(registry);
        
        this.eventProcessedCounter = Counter.builder("event.processed.total")
            .tag("event_type", "EventCreated")
            .register(registry);
    }
    
    public void recordProcessing(String eventType, Duration duration) {
        eventProcessingTimer.record(duration);
        eventProcessedCounter.increment();
    }
}
```

**Prometheus Query (p99 latency):**
```promql
histogram_quantile(0.99, 
  sum(rate(event_processing_duration_bucket{event_type="TicketPurchased"}[5m])) by (le)
)
```

**Alert Rule (SLA breach):**
```yaml
groups:
  - name: eventual_consistency_sla
    rules:
      - alert: TicketPurchaseLatencyHigh
        expr: |
          histogram_quantile(0.99,
            sum(rate(event_processing_duration_bucket{event_type="TicketPurchased"}[5m])) by (le)
          ) > 3
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Ticket purchase latency p99 > 3s (SLA breach)"
          description: "Current p99: {{ $value }}s"
```

---

## 4. Kafka Resilience Testing

Matryca testów odporności na awarie Kafka.

### 4.1 Scenariusze

| Scenariusz | Impact | Recovery Time | Test Status |
|------------|--------|---------------|-------------|
| **Broker Down** | Partition leader election | < 30s | ✅ |
| **Partition Loss** | Messages buffered | Manual intervention | ✅ |
| **Network Latency** | Slow processing | Automatic (retry) | ✅ |
| **Consumer Rebalance** | Processing pause | < 10s | ⏳ |
| **Producer Timeout** | Buffered in Outbox | < 60s | ✅ |
| **ZooKeeper Down** | (N/A - KRaft mode) | N/A | N/A |

### 4.2 Test - Broker Down

**Używając Testcontainers + Toxiproxy:**
```java
@Test
void shouldRecoverWhenKafkaBrokerGoesDown() {
    // Given: Kafka cluster (3 brokers)
    // When: Leader broker crashes
    kafkaContainer.stopBroker(0); // Kill broker-0
    
    // Then: Producer automatycznie przełącza na broker-1
    await().atMost(30, SECONDS).untilAsserted(() -> {
        ProducerMetrics metrics = kafkaProducer.metrics();
        assertThat(metrics.getActiveBroker()).isNotEqualTo(0);
    });
    
    // And: Messages nadal dostarczane
    UUID eventId = UUID.randomUUID();
    kafkaProducer.send("events.created", new EventCreatedEvent(eventId, ...));
    
    await().atMost(10, SECONDS).untilAsserted(() -> {
        assertThat(repository.findById(eventId)).isPresent();
    });
}
```

### 4.3 Test - Consumer Rebalance

**Cel:** Weryfikacja braku utraty/duplikacji podczas rebalansowania.

```java
@Test
void shouldHandleConsumerRebalanceWithoutMessageLoss() {
    // Given: Consumer Group z 1 instancją
    String groupId = "event-processors-test";
    ConsumerGroup group = new ConsumerGroup(groupId, 1);
    group.start();
    
    // When: Publikuj 1000 eventów
    List<UUID> eventIds = publishEvents(1000);
    
    // And: Dodaj drugą instancję consumer (trigger rebalance!)
    group.addInstance(); // Rebalance triggered
    
    // Then: Wszystkie 1000 eventów przetworzone dokładnie raz
    await().atMost(60, SECONDS).untilAsserted(() -> {
        long processedCount = repository.count();
        assertThat(processedCount).isEqualTo(1000);
    });
    
    // And: Brak duplikatów
    for (UUID eventId : eventIds) {
        long count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM events WHERE id = ?",
            Long.class,
            eventId
        );
        assertThat(count).isEqualTo(1);
    }
}
```

---

## 5. Outbox Pattern Test Scenarios

Matryca testów dla Transactional Outbox Pattern.

### 5.1 Scenariusze

| Scenariusz | Oczekiwane Zachowanie | Test Status |
|------------|------------------------|-------------|
| **Transakcja OK** | Event opublikowany | ✅ |
| **Rollback transakcji** | Event NIE opublikowany | ✅ |
| **Publisher restart** | Brak duplikatów | ⏳ |
| **Race condition** | Atomic lock | ⏳ |
| **Kafka unavailable** | Buffer w Outbox | ✅ |

### 5.2 Test - Rollback Scenario

```java
@Test
void shouldNotPublishEventWhenTransactionRollsBack() {
    // Given: Command który rzuci exception
    UUID eventId = UUID.randomUUID();
    CreateEventCommand command = new CreateEventCommand(eventId, "Test Event", null); // null date - invalid!
    
    // When: Try to process (will fail validation)
    assertThatThrownBy(() -> commandHandler.handle(command))
        .isInstanceOf(ValidationException.class);
    
    // Then: Event NIE został zapisany w Outbox
    long outboxCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox WHERE aggregate_id = ?",
        Long.class,
        eventId
    );
    assertThat(outboxCount).isZero();
    
    // And: Event NIE został opublikowany do Kafka
    await().during(5, SECONDS).atMost(6, SECONDS).untilAsserted(() -> {
        assertThat(kafkaTestConsumer.poll()).isEmpty();
    });
}
```

### 5.3 Test - Publisher Idempotency

```java
@Test
void shouldNotPublishDuplicatesAfterPublisherRestart() {
    // Given: Event zapisany w Outbox
    UUID eventId = UUID.randomUUID();
    EventCreatedEvent event = new EventCreatedEvent(eventId, ...);
    outboxRepository.save(new OutboxMessage(eventId, "EventCreated", event));
    
    // When: Publisher przetwarza
    outboxPublisher.publishPendingMessages();
    
    // And: Restart przed usunięciem rekordu z Outbox (symulacja crash)
    // (Outbox message ma status PUBLISHED ale nie został usunięty)
    
    // And: Publisher uruchamia się ponownie
    outboxPublisher.restart();
    outboxPublisher.publishPendingMessages();
    
    // Then: Event opublikowany tylko raz (Kafka key-based deduplication)
    List<ConsumerRecord<String, String>> records = kafkaTestConsumer.pollAll();
    assertThat(records).hasSize(1);
}
```

**Implementacja Idempotency:**
```java
@Service
public class OutboxPublisher {
    
    @Scheduled(fixedDelay = 1000) // Co 1s
    @Transactional
    public void publishPendingMessages() {
        List<OutboxMessage> pending = outboxRepository.findPendingWithLock();
        
        for (OutboxMessage message : pending) {
            try {
                // Użyj aggregate_id jako Kafka key (deduplikacja)
                kafkaTemplate.send(
                    message.getTopic(),
                    message.getAggregateId().toString(), // KEY!
                    message.getPayload()
                );
                
                // Mark as published (atomic with Kafka send - transactional producer)
                message.setStatus(OutboxStatus.PUBLISHED);
                message.setPublishedAt(Instant.now());
                outboxRepository.save(message);
                
            } catch (Exception e) {
                log.error("Failed to publish outbox message: {}", message.getId(), e);
                message.incrementRetryCount();
                outboxRepository.save(message);
            }
        }
    }
}
```

---

## 6. Dead Letter Queue Validation

Matryca walidacji DLQ.

### 6.1 Wymagane Nagłówki DLQ

Każdy message w DLQ **musi** zawierać:

| Nagłówek | Typ | Wymagane | Przykład |
|----------|-----|----------|----------|
| `dlq.original.topic` | String | ✅ | `events.created` |
| `dlq.original.partition` | Integer | ✅ | `2` |
| `dlq.original.offset` | Long | ✅ | `12345` |
| `dlq.error.message` | String | ✅ | `NullPointerException: name` |
| `dlq.error.stacktrace` | String | ✅ | `at com.example...` |
| `dlq.error.type` | String | ✅ | `NullPointerException` |
| `dlq.retry.count` | Integer | ✅ | `3` |
| `dlq.timestamp` | Long | ✅ | `1706644800000` |
| `dlq.consumer.group` | String | ✅ | `event-processors` |

### 6.2 Test - DLQ Headers Validation

```java
@Test
void shouldIncludeAllRequiredHeadersInDLQ() {
    // Given: Event z błędem (null name)
    String poisonJson = """
        {
          "eventId": "123",
          "name": null,
          "date": "2025-06-15"
        }
        """;
    
    // When: Consumer przetwarza (rzuci NullPointerException)
    kafkaProducer.send("events.created", poisonJson);
    
    // Then: Event w DLQ
    await().atMost(10, SECONDS).untilAsserted(() -> {
        ConsumerRecords<String, String> dlqRecords = dlqConsumer.poll(Duration.ofSeconds(1));
        assertThat(dlqRecords).isNotEmpty();
        
        ConsumerRecord<String, String> dlqRecord = dlqRecords.iterator().next();
        
        // Validate all required headers
        Map<String, String> headers = extractHeaders(dlqRecord);
        assertThat(headers).containsKeys(
            "dlq.original.topic",
            "dlq.original.partition",
            "dlq.original.offset",
            "dlq.error.message",
            "dlq.error.stacktrace",
            "dlq.error.type",
            "dlq.retry.count",
            "dlq.timestamp",
            "dlq.consumer.group"
        );
        
        // Validate specific values
        assertThat(headers.get("dlq.original.topic")).isEqualTo("events.created");
        assertThat(headers.get("dlq.error.type")).isEqualTo("NullPointerException");
        assertThat(headers.get("dlq.retry.count")).isEqualTo("3");
    });
}
```

### 6.3 DLQ SLA Matrix

| Event Criticality | Max Time to Investigation | Max Time to Resolution | Auto-Retry |
|-------------------|---------------------------|------------------------|------------|
| **CRITICAL** (Payment) | 15 min | 1h | ✅ (3x) |
| **HIGH** (Ticket) | 1h | 4h | ✅ (3x) |
| **MEDIUM** (Event) | 4h | 24h | ✅ (2x) |
| **LOW** (Email) | 24h | 7 days | ❌ |

**Alert Prometheus:**
```yaml
- alert: CriticalEventInDLQ
  expr: kafka_dlq_messages_total{topic="payments.processed"} > 0
  for: 5m
  labels:
    severity: critical
    pager: true
  annotations:
    summary: "Critical payment event in DLQ"
```

---

## 7. Security Testing Matrix

Matryca testów bezpieczeństwa.

### 7.1 Scenariusze

| Aspekt | Test | Status |
|--------|------|--------|
| **Kafka ACL** | Unauthorized producer blocked | ⏳ |
| **TLS Encryption** | All connections use TLS 1.3 | ⏳ |
| **Secret Rotation** | Zero downtime during rotation | ⏳ |
| **PII Masking** | Personal data masked in logs | ⏳ |
| **GDPR Compliance** | Right to be forgotten | ⏳ |

### 7.2 Test - Kafka ACL

```java
@Test
void shouldDenyUnauthorizedProducerAccess() {
    // Given: Producer bez ACL do topicu "payments"
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.jaas.config", "... hacker credentials ...");
    
    KafkaProducer<String, String> unauthorizedProducer = new KafkaProducer<>(props);
    
    // When/Then: Publish rzuca AuthorizationException
    assertThatThrownBy(() -> {
        ProducerRecord<String, String> record = new ProducerRecord<>(
            "payments.processed", 
            "test"
        );
        unauthorizedProducer.send(record).get(5, TimeUnit.SECONDS);
    }).isInstanceOf(ExecutionException.class)
      .hasCauseInstanceOf(AuthorizationException.class);
}
```

---

## 8. Performance Benchmarks

### 8.1 Docelowe Metryki

| Metryka | Target | Measured | Status |
|---------|--------|----------|--------|
| **Throughput (Commands)** | 1000 req/s | TBD | ⏳ |
| **Throughput (Events)** | 5000 msg/s | TBD | ⏳ |
| **API Latency p99** | < 500ms | TBD | ⏳ |
| **Event Processing p99** | < 300ms | TBD | ⏳ |
| **Error Rate** | < 0.1% | TBD | ⏳ |

### 8.2 Test - K6 Load Test

```javascript
// k6-load-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '2m', target: 100 },  // Ramp-up
    { duration: '5m', target: 100 },  // Steady
    { duration: '2m', target: 0 },    // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(99)<500'],  // 99% < 500ms
    http_req_failed: ['rate<0.001'],   // < 0.1% errors
  },
};

export default function () {
  let res = http.post(
    'http://localhost:8080/api/events',
    JSON.stringify({
      name: `Test Event ${__VU}-${__ITER}`,
      date: '2025-12-01',
      location: 'Warsaw',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, {
    'status 201': (r) => r.status === 201,
    'latency < 500ms': (r) => r.timings.duration < 500,
  });

  sleep(1);
}
```

**Run:**
```bash
k6 run --vus 100 --duration 10m k6-load-test.js
```

---

## 9. Chaos Engineering Scenarios

### 9.1 Scenariusze Toxiproxy

| Chaos | Tool | Duration | Expected Behavior |
|-------|------|----------|-------------------|
| **Network Latency (100ms)** | Toxiproxy | 5 min | Timeouts, Circuit Breaker opens |
| **Postgres Down** | Docker stop | 30s | 503 Service Unavailable |
| **Kafka Broker Down** | Docker stop | 1 min | Buffered in Outbox |
| **Random Packet Loss (10%)** | Toxiproxy | 2 min | Retries, eventual success |

### 9.2 Test - Network Latency

```java
@Test
void shouldOpenCircuitBreakerUnderHighLatency() {
    // Given: Toxiproxy przed Postgres
    Proxy postgresProxy = toxiproxyClient.getProxy("postgres");
    
    // When: Dodaj 5s opóźnienia
    postgresProxy.toxics().latency("slow-db", ToxicDirection.DOWNSTREAM, 5000);
    
    // Then: Circuit Breaker otwiera się po 3 nieudanych próbach
    for (int i = 0; i < 5; i++) {
        assertThatThrownBy(() -> repository.findAll())
            .isInstanceOf(CircuitBreakerOpenException.class);
    }
    
    // When: Usuń latency
    postgresProxy.toxics().get("slow-db").remove();
    
    // Then: Circuit Breaker zamyka się po timeout (half-open → closed)
    await().atMost(30, SECONDS).untilAsserted(() -> {
        assertThat(repository.findAll()).isNotNull();
    });
}
```

---

## 10. Contract Testing Matrix

### 10.1 Kontrakt Provider (Event Service) → Consumer (Analytics Service)

| Kontrakt | Provider | Consumer | Status |
|----------|----------|----------|--------|
| **EventPublicationNotified** | Event Service | Analytics Service | ✅ |
| **TicketPurchaseCompleted** | Ticket Service | Analytics Service | ⏳ |
| **PaymentProcessed** | Payment Service | Ticket Service | ⏳ |

### 10.2 Pact Test (Provider Side)

```java
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
@Provider("event-service")
@PactBroker(url = "http://localhost:9292")
public class EventServiceContractTest {
    
    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void pactVerificationTestTemplate(PactVerificationContext context) {
        context.verifyInteraction();
    }
    
    @State("event exists and is published")
    public void eventExistsAndPublished() {
        // Setup: Create event in PUBLISHED state
        eventRepository.save(new Event(
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
            "Spring Conference",
            EventStatus.PUBLISHED,
            Instant.now()
        ));
    }
}
```

---

## 📊 Status Ogólny

### Coverage by Category

| Kategoria | Testy Zaimplementowane | Testy Planowane | Coverage |
|-----------|------------------------|-----------------|----------|
| **Event Processing** | 18 | 24 | 75% |
| **Domain vs Integration** | 6 | 8 | 75% |
| **Eventual Consistency** | 8 | 10 | 80% |
| **Kafka Resilience** | 12 | 15 | 80% |
| **Outbox Pattern** | 10 | 12 | 83% |
| **DLQ** | 8 | 10 | 80% |
| **Security** | 2 | 10 | 20% ⚠️ |
| **Performance** | 4 | 8 | 50% ⚠️ |
| **Chaos Engineering** | 6 | 12 | 50% ⚠️ |
| **Contract Testing** | 3 | 6 | 50% ⚠️ |
| **TOTAL** | **77** | **115** | **67%** |

### Priority Next Steps

1. ⚠️ **Security Testing** (20% → 80%) - krytyczne dla produkcji
2. ⚠️ **Chaos Engineering** (50% → 90%) - odporność systemu
3. ⚠️ **Performance Benchmarks** (50% → 100%) - SLA compliance
4. ⚠️ **Contract Testing** (50% → 90%) - multi-service integrity

---

**Koniec Testing Matrix** 🧪
