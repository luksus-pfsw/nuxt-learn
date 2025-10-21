# 🎓 CZĘŚĆ I: FUNDAMENTY Message & Event-Driven Architecture

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Apache Kafka + Spring Boot 3.4 + Nuxt 3  
**Cel:** Zrozumienie fundamentów architektury systemów rozproszonych  
**Czas nauki:** 3-4 tygodnie (Tematy 1-10)

---

## 📋 Spis Treści - CZĘŚĆ I

1. [Message-Driven Architecture](#temat-1-message-driven-architecture)
2. [Event-Driven Architecture (EDA)](#temat-2-event-driven-architecture)
3. [CQRS - Rozdzielenie Zapisu i Odczytu](#temat-3-cqrs)
4. [Commands vs Events vs Queries](#temat-4-commands-vs-events-vs-queries)
5. [Bounded Context](#temat-5-bounded-context)
6. [Aggregate - Granica Spójności](#temat-6-aggregate)
7. [Domain Events](#temat-7-domain-events)
8. [Eventual Consistency](#temat-8-eventual-consistency)
9. [Idempotency](#temat-9-idempotency)
10. [Saga Pattern](#temat-10-saga-pattern)

---

## 🎯 Cele Nauki - CZĘŚĆ I

Po ukończeniu Części I będziesz:
- ✅ Rozumieć fundamenty Message-Driven Architecture
- ✅ Znać różnice między Commands, Events i Queries
- ✅ Implementować CQRS w praktyce
- ✅ Projektować Bounded Contexts
- ✅ Obsługiwać Eventual Consistency w testach
- ✅ Pisać idempotentne handlery
- ✅ Implementować wzorzec Saga

---

## Temat 1: Message-Driven Architecture

### 1.1 Definicja

**Message-Driven Architecture** to styl architektoniczny, w którym komponenty systemu komunikują się poprzez **wymianę wiadomości** (messages) zamiast bezpośrednich wywołań.

### 1.2 Analogia: System Pocztowy

**Tradycyjne podejście (synchroniczne):**
```
Ty → Dzwonisz do znajomego → Czekasz na odpowiedź
```
Problemy:
- Musisz czekać aż odbierze
- Jeśli nie ma go w domu, tracisz czas
- Jeśli zajęty, nie możesz nic zrobić

**Message-Driven (asynchroniczne):**
```
Ty → Wysyłasz SMS/Email → Robisz swoje rzeczy
Znajomy → Odbiera gdy ma czas → Odpowiada
```
Zalety:
- Nie czekasz na odpowiedź
- Znajomy może być offline
- Możesz wysłać wiele wiadomości jednocześnie


### 1.3 W EventMaster - Praktyczny Przykład

#### Architektura

```
┌─────────────┐
│   Frontend  │ (Nuxt 3)
│   (Browser) │
└──────┬──────┘
       │ HTTP POST /api/v1/events
       ↓
┌─────────────────┐
│  Backend API    │ (Spring Boot 3.4)
│  @RestController│
└────────┬────────┘
         │ Publish Message
         ↓
┌──────────────────────────┐
│   Apache Kafka           │
│   Topic: commands.events │
└────────┬─────────────────┘
         │ Consume Message
         ↓
┌────────────────────┐
│  Command Handler   │
│  (Business Logic)  │
└─────────┬──────────┘
          │ Save to DB
          ↓
┌─────────────────┐
│  PostgreSQL 18  │
│  Table: events  │
└─────────────────┘
```

#### Kod - Producer (Wysyłanie Wiadomości)

**backend/src/main/java/com/eventmaster/command/api/EventCommandController.java**
```java
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventCommandController {
    
    private final KafkaTemplate<String, CreateEventCommand> kafkaTemplate;
    
    @PostMapping
    public ResponseEntity<CommandResponse> createEvent(
        @RequestBody @Valid CreateEventCommandDTO dto
    ) {
        // 1. Utworzenie komendy
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            dto.getName(),
            dto.getDescription(),
            dto.getLocation(),
            dto.getStartDate()
        );
        
        // 2. Wysłanie do Kafki (asynchroniczne!)
        kafkaTemplate.send(
            "commands.events.create", 
            command.getEventId().toString(), 
            command
        );
        
        // 3. Natychmiastowa odpowiedź (202 Accepted)
        return ResponseEntity.accepted()
            .body(new CommandResponse(command.getEventId(), "ACCEPTED"));
    }
}
```

#### Kod - Consumer (Odbieranie Wiadomości)

**backend/src/main/java/com/eventmaster/command/handler/CreateEventCommandHandler.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class CreateEventCommandHandler {
    
    private final EventRepository eventRepository;
    private final KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @KafkaListener(
        topics = "commands.events.create",
        groupId = "event-command-handlers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void handle(CreateEventCommand command) {
        log.info("Processing CreateEventCommand: {}", command.getEventId());
        
        // 1. Walidacja
        validateCommand(command);
        
        // 2. Utworzenie aggregate
        Event event = Event.create(
            command.getEventId(),
            command.getName(),
            command.getDescription(),
            command.getLocation(),
            command.getStartDate()
        );
        
        // 3. Zapis do PostgreSQL
        eventRepository.save(event);
        log.info("Event saved to database: {}", event.getId());
        
        // 4. Publikacja event (fakt, że coś się stało)
        EventCreatedEvent eventCreated = new EventCreatedEvent(
            event.getId(),
            event.getName(),
            event.getDescription(),
            event.getLocation(),
            event.getStartDate(),
            Instant.now()
        );
        
        eventPublisher.send(
            "events.lifecycle",
            event.getId().toString(),
            eventCreated
        );
        
        log.info("EventCreatedEvent published: {}", event.getId());
    }
    
    private void validateCommand(CreateEventCommand command) {
        if (command.getName() == null || command.getName().isBlank()) {
            throw new ValidationException("Event name is required");
        }
        if (command.getStartDate().isBefore(LocalDateTime.now())) {
            throw new ValidationException("Event cannot start in the past");
        }
    }
}
```

### 1.4 Dlaczego Message-Driven?

#### Porównanie: Synchroniczne vs Message-Driven

**❌ Synchroniczne (Tradycyjne)**
```java
@PostMapping("/events")
public ResponseEntity<EventDTO> createEvent(@RequestBody CreateEventDTO dto) {
    // 1. Zapis do bazy
    Event event = eventService.save(dto);  // 100ms
    
    // 2. Wysłanie emaila
    emailService.sendConfirmation(event);  // 500ms (WOLNE!)
    
    // 3. Aktualizacja analytics
    analyticsService.track(event);         // 200ms
    
    // 4. Indeksowanie w Elasticsearch
    searchService.index(event);            // 300ms
    
    // TOTAL: 1100ms! Użytkownik czeka! 😱
    return ResponseEntity.ok(event);
}
```

**✅ Message-Driven (Asynchroniczne)**
```java
@PostMapping("/events")
public ResponseEntity<CommandResponse> createEvent(@RequestBody CreateEventDTO dto) {
    // 1. Wysłanie komendy do Kafki
    kafkaTemplate.send("commands.events.create", command);  // 5ms
    
    // 2. Natychmiastowa odpowiedź!
    return ResponseEntity.accepted()
        .body(new CommandResponse("ACCEPTED"));
    // TOTAL: 5ms! Użytkownik szczęśliwy! 🚀
}

// W tle (asynchronicznie):
// - Command Handler zapisze do bazy
// - Email Service wyśle email
// - Analytics Service zatrackuje
// - Search Service zaindeksuje
// Wszystko równolegle, bez blokowania użytkownika!
```

### 1.5 Zalety Message-Driven Architecture

1. **Decoupling (Rozdzielenie)**
   - Producer nie zna Consumer
   - Możemy dodawać nowych Consumers bez zmiany Producera

2. **Scalability (Skalowalność)**
   - Kafka może obsłużyć miliony messages/sekundę
   - Łatwo dodać więcej Consumers (horizontal scaling)

3. **Resilience (Odporność na awarie)**
   - Jeśli Consumer pada, message pozostaje w Kafce
   - Po restarcie Consumer odbierze zaległe messages

4. **Performance (Wydajność)**
   - User dostaje odpowiedź natychmiast (202 Accepted)
   - Długie operacje dzieją się w tle

### 1.6 Testowanie Message-Driven Architecture

#### Test 1: Producer Publikuje Message

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"commands.events.create"})
class EventCommandControllerTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    private KafkaConsumer<String, CreateEventCommand> testConsumer;
    
    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
            "test-group", "true", embeddedKafka
        );
        testConsumer = new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(CreateEventCommand.class)
        );
        testConsumer.subscribe(List.of("commands.events.create"));
    }
    
    @Test
    void shouldPublishCommandToKafka() throws Exception {
        // Given
        String requestBody = """
            {
                "name": "JavaConf 2025",
                "description": "Biggest Java conference",
                "location": "Warsaw, Poland",
                "startDate": "2025-06-15T09:00:00"
            }
            """;
        
        // When
        mockMvc.perform(post("/api/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(requestBody))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.status").value("ACCEPTED"));
        
        // Then - sprawdź czy message trafił do Kafki
        ConsumerRecords<String, CreateEventCommand> records = 
            KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
        
        assertThat(records.count()).isEqualTo(1);
        
        ConsumerRecord<String, CreateEventCommand> record = records.iterator().next();
        CreateEventCommand command = record.value();
        
        assertThat(command.getName()).isEqualTo("JavaConf 2025");
        assertThat(command.getDescription()).isEqualTo("Biggest Java conference");
    }
}
```

#### Test 2: Consumer Przetwarza Message

```java
@SpringBootTest
@Testcontainers
class CreateEventCommandHandlerTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18")
        .withDatabaseName("eventmaster_test")
        .withUsername("test")
        .withPassword("test");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private CreateEventCommandHandler handler;
    
    @Autowired
    private EventRepository eventRepository;
    
    @Autowired
    private KafkaTemplate<String, CreateEventCommand> commandPublisher;
    
    @Test
    void shouldCreateEventWhenCommandReceived() {
        // Given
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            "SpringOne 2025",
            "Spring Framework conference",
            "San Francisco, USA",
            LocalDateTime.of(2025, 8, 20, 9, 0)
        );
        
        // When - publish command
        commandPublisher.send(
            "commands.events.create", 
            command.getEventId().toString(), 
            command
        );
        
        // Then - wait for processing (eventual consistency!)
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Event> events = eventRepository.findAll();
            assertThat(events).hasSize(1);
            
            Event event = events.get(0);
            assertThat(event.getName()).isEqualTo("SpringOne 2025");
            assertThat(event.getStatus()).isEqualTo(EventStatus.DRAFT);
        });
    }
    
    @Test
    void shouldRejectCommandWithInvalidData() {
        // Given - komenda z pustą nazwą
        CreateEventCommand invalidCommand = new CreateEventCommand(
            UUID.randomUUID(),
            "",  // Pusta nazwa!
            "Description",
            "Location",
            LocalDateTime.now().plusDays(1)
        );
        
        // When / Then
        assertThatThrownBy(() -> handler.handle(invalidCommand))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Event name is required");
        
        // Event NIE został zapisany
        assertThat(eventRepository.findAll()).isEmpty();
    }
}
```

### 1.7 Konfiguracja Apache Kafka + PostgreSQL 18

#### Docker Compose

**docker/docker-compose.yml**
```yaml
version: '3.9'

services:
  # Apache Kafka (KRaft mode - bez Zookeeper)
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: eventmaster-kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - kafka_data:/var/lib/kafka/data

  # PostgreSQL 18
  postgres:
    image: postgres:18
    container_name: eventmaster-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: eventmaster
      POSTGRES_USER: eventmaster
      POSTGRES_PASSWORD: eventmaster123
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U eventmaster"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Kafka UI (opcjonalne - do monitorowania)
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

volumes:
  kafka_data:
  postgres_data:
```

#### Spring Boot Configuration

**backend/src/main/resources/application.yml**
```yaml
spring:
  application:
    name: eventmaster-backend
  
  # PostgreSQL 18 Configuration
  datasource:
    url: jdbc:postgresql://localhost:5432/eventmaster
    username: eventmaster
    password: eventmaster123
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5
      connection-timeout: 20000
  
  jpa:
    database-platform: org.hibernate.dialect.PostgreSQLDialect
    hibernate:
      ddl-auto: validate
    show-sql: true
    properties:
      hibernate:
        format_sql: true
        jdbc:
          time_zone: UTC
        default_schema: public
  
  # Apache Kafka Configuration
  kafka:
    bootstrap-servers: localhost:9092
    
    # Producer settings
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        linger.ms: 10
        compression.type: snappy
    
    # Consumer settings
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      group-id: eventmaster-backend
      auto-offset-reset: earliest
      enable-auto-commit: false
      properties:
        spring.json.trusted.packages: com.eventmaster.*
        isolation.level: read_committed
    
    # Listener settings
    listener:
      ack-mode: manual
      concurrency: 3

# Flyway migrations
flyway:
  enabled: true
  locations: classpath:db/migration
  baseline-on-migrate: true

# Application properties
eventmaster:
  kafka:
    topics:
      commands:
        events:
          create: commands.events.create
          update: commands.events.update
          delete: commands.events.delete
      events:
        lifecycle: events.lifecycle
```

### 1.8 Pułapki dla Testera

#### Pułapka 1: At-Least-Once Delivery

Kafka gwarantuje **przynajmniej raz** - message może przyjść 2 razy!

```java
// ❌ ZŁY KOD - tworzy duplikaty
@KafkaListener(topics = "commands.events.create")
public void handle(CreateEventCommand command) {
    eventRepository.save(new Event(command));
    // Jeśli Kafka dostarcza message 2x → 2 eventy w bazie!
}

// ✅ DOBRY KOD - idempotentny
@KafkaListener(topics = "commands.events.create")
public void handle(CreateEventCommand command) {
    if (eventRepository.existsById(command.getEventId())) {
        log.info("Event already exists, skipping");
        return;
    }
    eventRepository.save(new Event(command));
}
```

#### Pułapka 2: Eventual Consistency

Read Model może być nieaktualny przez kilka sekund!

```java
// ❌ ZŁY TEST
@Test
void badTest() {
    kafkaTemplate.send("commands.events.create", command);
    Event event = repository.findById(id).get();  // FAIL! Jeszcze nie istnieje!
}

// ✅ DOBRY TEST
@Test
void goodTest() {
    kafkaTemplate.send("commands.events.create", command);
    await().atMost(5, SECONDS).untilAsserted(() -> {
        Optional<Event> event = repository.findById(id);
        assertThat(event).isPresent();
    });
}
```

### 1.9 Ćwiczenie Praktyczne

**Zadanie:** Dodaj endpoint do anulowania wydarzenia.

1. Utwórz `CancelEventCommand`
2. Dodaj endpoint `DELETE /api/v1/events/{id}`
3. Utwórz `CancelEventCommandHandler`
4. Opublikuj `EventCancelledEvent`
5. Napisz testy

### 1.10 Podsumowanie

**Kluczowe Pojęcia:**
- Message-Driven = asynchroniczna komunikacja
- Producer wysyła, Consumer odbiera
- Kafka = message broker
- PostgreSQL 18 = write store
- Testcontainers = testy integracyjne

**Następny temat:** [Event-Driven Architecture](#temat-2-event-driven-architecture)

---

## Temat 2: Event-Driven Architecture (EDA)

### 2.1 Definicja

**Event-Driven Architecture** to specjalizacja Message-Driven, gdzie messages są **zdarzeniami** (events) - faktami o tym, co się już wydarzyło.

### 2.2 Różnica: Command vs Event

| Aspekt | Command | Event |
|--------|---------|-------|
| **Czas** | Przyszłość (intent) | Przeszłość (fact) |
| **Nazewnictwo** | CreateEvent | EventCreated |
| **Odrzucenie** | Możliwe | Niemożliwe (już się stało) |
| **Odbiorcy** | Jeden (directed) | Wielu (broadcast) |
| **Zmiana** | Można zmienić | Immutable |

### 2.3 Analogia

**Command:**
```
Szef: "Napisz raport!" (rozkaz)
Ty: "Nie mam czasu" (możesz odmówić)
```

**Event:**
```
Ty: "Raport został napisany" (fakt)
Szef: (musi zaakceptować, już się stało)
Marketing: (też może zareagować)
HR: (też się dowiaduje)
```

### 2.4 W EventMaster - Event Flow

```
┌──────────────────────────────────────────┐
│  1. User Action                          │
│  POST /api/v1/events                     │
└────────────────┬─────────────────────────┘
                 ↓
┌──────────────────────────────────────────┐
│  2. Command Published                    │
│  Topic: commands.events.create           │
│  Message: CreateEventCommand             │
└────────────────┬─────────────────────────┘
                 ↓
┌──────────────────────────────────────────┐
│  3. Command Handler                      │
│  - Validates                             │
│  - Saves to PostgreSQL                   │
│  - Publishes Event                       │
└────────────────┬─────────────────────────┘
                 ↓
┌──────────────────────────────────────────┐
│  4. Event Published                      │
│  Topic: events.lifecycle                 │
│  Message: EventCreatedEvent              │
└────┬────────┬────────┬────────┬──────────┘
     │        │        │        │
     ↓        ↓        ↓        ↓
┌─────────┐ ┌──────┐ ┌────────┐ ┌─────────┐
│Projector│ │Email │ │Analytics│ │Search   │
│         │ │Service│ │Service │ │Service  │
└─────────┘ └──────┘ └────────┘ └─────────┘
```

### 2.5 Implementacja - Event

**backend/src/main/java/com/eventmaster/events/EventCreatedEvent.java**
```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCreatedEvent {
    private UUID eventId;
    private String name;
    private String description;
    private String location;
    private LocalDateTime startDate;
    private Instant occurredAt;  // Kiedy się wydarzyło
    
    // Events są IMMUTABLE - tylko gettery!
    // NO setters after construction!
}
```

### 2.6 Implementacja - Multiple Listeners

#### Listener 1: Projector (Read Model)

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventViewProjector {
    
    private final EventViewRepository repository;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting EventCreatedEvent: {}", event.getEventId());
        
        EventView view = new EventView();
        view.setId(event.getEventId());
        view.setName(event.getName());
        view.setDescription(event.getDescription());
        view.setLocation(event.getLocation());
        view.setStartDate(event.getStartDate());
        view.setStatus("DRAFT");
        view.setCreatedAt(event.getOccurredAt());
        
        repository.save(view);
        log.info("EventView created: {}", view.getId());
    }
}
```

#### Listener 2: Email Service

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventEmailService {
    
    private final EmailSender emailSender;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-email-service"
    )
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Sending confirmation email for event: {}", event.getEventId());
        
        Email email = Email.builder()
            .to("organizer@example.com")
            .subject("Event Created: " + event.getName())
            .body("Your event has been created successfully!")
            .build();
        
        emailSender.send(email);
        log.info("Email sent for event: {}", event.getEventId());
    }
}
```

#### Listener 3: Analytics Service

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventAnalyticsService {
    
    private final AnalyticsRepository analyticsRepository;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-analytics-service"
    )
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Tracking analytics for event: {}", event.getEventId());
        
        EventAnalytics analytics = new EventAnalytics();
        analytics.setEventId(event.getEventId());
        analytics.setEventType("conference");
        analytics.setCreatedAt(event.getOccurredAt());
        analytics.setViewCount(0L);
        
        analyticsRepository.save(analytics);
        log.info("Analytics tracked for event: {}", event.getEventId());
    }
}
```

### 2.7 Testowanie Event-Driven Architecture

#### Test 1: Event jest Publikowany

```java
@SpringBootTest
@Testcontainers
class EventCreatedEventPublishingTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private KafkaTemplate<String, CreateEventCommand> commandPublisher;
    
    private KafkaConsumer<String, EventCreatedEvent> eventConsumer;
    
    @BeforeEach
    void setUp() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        eventConsumer = new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(EventCreatedEvent.class)
        );
        eventConsumer.subscribe(List.of("events.lifecycle"));
    }
    
    @Test
    void shouldPublishEventAfterCommandProcessing() {
        // Given
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            "DockerCon 2025",
            "Container conference",
            "Seattle, USA",
            LocalDateTime.of(2025, 9, 15, 9, 0)
        );
        
        // When
        commandPublisher.send("commands.events.create", command);
        
        // Then - czekaj na event
        await().atMost(10, SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, EventCreatedEvent> records = 
                eventConsumer.poll(Duration.ofMillis(100));
            
            assertThat(records.count()).isGreaterThan(0);
            
            EventCreatedEvent event = records.iterator().next().value();
            assertThat(event.getEventId()).isEqualTo(command.getEventId());
            assertThat(event.getName()).isEqualTo("DockerCon 2025");
            assertThat(event.getOccurredAt()).isNotNull();
        });
    }
}
```

#### Test 2: Multiple Listeners Reagują

```java
@SpringBootTest
@Testcontainers
class MultipleListenersTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @Autowired
    private EventViewRepository viewRepository;
    
    @Autowired
    private AnalyticsRepository analyticsRepository;
    
    @SpyBean
    private EventEmailService emailService;
    
    @Test
    void shouldTriggerAllListeners() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "KubeCon 2025",
            "Kubernetes conference",
            "Amsterdam, NL",
            LocalDateTime.of(2025, 5, 10, 9, 0),
            Instant.now()
        );
        
        // When
        eventPublisher.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then - wszystkie 3 listenery powinny zareagować
        await().atMost(10, SECONDS).untilAsserted(() -> {
            // 1. Projector zaktualizował Read Model
            Optional<EventView> view = viewRepository.findById(event.getEventId());
            assertThat(view).isPresent();
            assertThat(view.get().getName()).isEqualTo("KubeCon 2025");
            
            // 2. Analytics został zatrackowany
            Optional<EventAnalytics> analytics = 
                analyticsRepository.findByEventId(event.getEventId());
            assertThat(analytics).isPresent();
            
            // 3. Email został wysłany
            verify(emailService, times(1)).onEventCreated(any(EventCreatedEvent.class));
        });
    }
}
```

### 2.8 Zalety Event-Driven Architecture

1. **Loose Coupling**
   - Komponenty nie znają się nawzajem
   - Dodanie nowego listenera nie wymaga zmian w innych

2. **Flexibility**
   - Łatwo dodać nową funkcjonalność (nowy listener)
   - Łatwo wyłączyć funkcjonalność (stop listener)

3. **Scalability**
   - Każdy listener może skalować się niezależnie
   - Analytics może mieć 10 instancji, Email 2 instancje

4. **Audit Trail**
   - Pełna historia zdarzeń w Kafce
   - Możliwość replay (odtworzenia stanu)

### 2.9 Dlaczego Apache Kafka?

EventMaster używa Apache Kafka jako głównej magistrali komunikacyjnej.

**Zalety Apache Kafka:**
- ✅ Dojrzały ekosystem i ekosystem narzędzi
- ✅ Wysoka wydajność i skalowalność
- ✅ Kafka Connect, Kafka Streams, ksqlDB
- ✅ Doskonała dokumentacja
- ✅ Ogromna społeczność developerska
- ✅ Production-ready w największych firmach świata
- ✅ KRaft mode (bez Zookeeper od wersji 3.x)

**Główne komponenty:**
- **Broker** - serwer Kafka przechowujący messages
- **Producer** - aplikacja wysyłająca messages
- **Consumer** - aplikacja odbierająca messages
- **Topic** - logiczny kanał dla messages
- **Partition** - fizyczny podział topicu (paralelizacja)

### 2.10 Ćwiczenie Praktyczne

**Zadanie:** Dodaj listener, który śledzi zmiany statusu wydarzenia.

1. Utwórz `EventStatusTracker`
2. Słuchaj na `events.lifecycle`
3. Zapisuj historię zmian statusu do tabeli `event_status_history`
4. Napisz test weryfikujący zapis

### 2.11 Podsumowanie

**Kluczowe Pojęcia:**
- Event = immutable fact
- Broadcast = wiele listenerów
- Decoupling = niezależne komponenty
- Kafka Topics = kanały komunikacji

**Następny temat:** [CQRS](#temat-3-cqrs)

---

## Temat 3: CQRS - Rozdzielenie Zapisu i Odczytu

### 3.1 Definicja

**CQRS** (Command Query Responsibility Segregation) to wzorzec architektoniczny dzielący system na:
- **Write Side** - optymalizowany pod zapis (Commands)
- **Read Side** - optymalizowany pod odczyt (Queries)

### 3.2 Problem: One Size Doesn't Fit All

**Tradycyjny Model (CRUD):**
```sql
-- Write operation (Complex)
INSERT INTO events (id, name, description, organizer_id, location_id)
VALUES (?, ?, ?, ?, ?);

-- Read operation (Requires JOINs)
SELECT 
    e.id,
    e.name,
    e.description,
    u.first_name || ' ' || u.last_name as organizer_name,
    l.city,
    l.country,
    COUNT(t.id) as ticket_count
FROM events e
JOIN users u ON e.organizer_id = u.id
JOIN locations l ON e.location_id = l.id
LEFT JOIN tickets t ON e.id = t.event_id
GROUP BY e.id, u.id, l.id;
-- Wolne dla 10,000 rekordów! 😱
```

**CQRS Solution:**
```sql
-- Write Side: Normalized (3NF)
Table: events
- id
- name
- organizer_id (FK)
- location_id (FK)

-- Read Side: Denormalized (Fast!)
Table: event_view
- id
- name
- organizer_name  -- Już połączone!
- city
- country
- ticket_count    -- Pre-calculated!

-- Query: 
SELECT * FROM event_view WHERE id = ?;
-- Błyskawiczne! ⚡
```

### 3.3 W EventMaster - Architektura CQRS

```
┌─────────────────────────────────────────────────┐
│                 WRITE SIDE                       │
│  ┌───────────────────────────────────────────┐ │
│  │  Table: events (PostgreSQL)               │ │
│  │  - id: UUID                               │ │
│  │  - name: VARCHAR                          │ │
│  │  - description: TEXT                      │ │
│  │  - location: VARCHAR                      │ │
│  │  - start_date: TIMESTAMP                  │ │
│  │  - status: VARCHAR                        │ │
│  │  - created_at: TIMESTAMP                  │ │
│  │  - updated_at: TIMESTAMP                  │ │
│  │  - version: BIGINT (optimistic locking)  │ │
│  └───────────────────────────────────────────┘ │
│          ↑ Commands (CreateEvent, UpdateEvent)  │
└──────────┼──────────────────────────────────────┘
           │
           │ EventCreatedEvent
           │ (Kafka: events.lifecycle)
           ↓
┌─────────────────────────────────────────────────┐
│                 READ SIDE                        │
│  ┌───────────────────────────────────────────┐ │
│  │  Table: event_view (PostgreSQL)           │ │
│  │  - id: UUID                               │ │
│  │  - name: VARCHAR                          │ │
│  │  - short_description: VARCHAR(200)        │ │
│  │  - location: VARCHAR                      │ │
│  │  - start_date: TIMESTAMP                  │ │
│  │  - formatted_date: VARCHAR                │ │
│  │  - status: VARCHAR                        │ │
│  │  - is_published: BOOLEAN                  │ │
│  │  - created_at: TIMESTAMP                  │ │
│  └───────────────────────────────────────────┘ │
│          ↑ Queries (GetEvent, ListEvents)       │
└─────────────────────────────────────────────────┘
```

### 3.4 Implementacja - Write Model

**backend/src/main/java/com/eventmaster/domain/Event.java**
```java
@Entity
@Table(name = "events")
@Data
@NoArgsConstructor
public class Event {
    
    @Id
    private UUID id;
    
    @Column(nullable = false, length = 200)
    private String name;
    
    @Column(columnDefinition = "TEXT")
    private String description;
    
    @Column(nullable = false)
    private String location;
    
    @Column(name = "start_date", nullable = false)
    private LocalDateTime startDate;
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;
    
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;
    
    @Column(name = "updated_at")
    private Instant updatedAt;
    
    @Version
    private Long version;  // Optimistic locking
    
    // Factory method
    public static Event create(
        UUID id, 
        String name, 
        String description, 
        String location, 
        LocalDateTime startDate
    ) {
        Event event = new Event();
        event.id = id;
        event.name = name;
        event.description = description;
        event.location = location;
        event.startDate = startDate;
        event.status = EventStatus.DRAFT;
        event.createdAt = Instant.now();
        return event;
    }
    
    // Business methods
    public void publish() {
        if (status == EventStatus.PUBLISHED) {
            throw new IllegalStateException("Event already published");
        }
        this.status = EventStatus.PUBLISHED;
        this.updatedAt = Instant.now();
    }
    
    public void cancel() {
        if (status == EventStatus.CANCELLED) {
            throw new IllegalStateException("Event already cancelled");
        }
        this.status = EventStatus.CANCELLED;
        this.updatedAt = Instant.now();
    }
}
```

### 3.5 Implementacja - Read Model

**backend/src/main/java/com/eventmaster/query/model/EventView.java**
```java
@Entity
@Table(name = "event_view")
@Data
@NoArgsConstructor
public class EventView {
    
    @Id
    private UUID id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(name = "short_description", length = 200)
    private String shortDescription;  // Denormalized!
    
    @Column(nullable = false)
    private String location;
    
    @Column(name = "start_date")
    private LocalDateTime startDate;
    
    @Column(name = "formatted_date")
    private String formattedDate;  // Pre-formatted! "15 Jun 2025, 09:00"
    
    @Column(nullable = false)
    private String status;
    
    @Column(name = "is_published")
    private Boolean isPublished;  // Derived field!
    
    @Column(name = "created_at")
    private Instant createdAt;
    
    // Read-only - NO business logic!
    // NO setters after projection!
}
```

### 3.6 Implementacja - Projector

**backend/src/main/java/com/eventmaster/query/projector/EventViewProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventViewProjector {
    
    private final EventViewRepository repository;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting EventCreatedEvent: {}", event.getEventId());
        
        EventView view = new EventView();
        view.setId(event.getEventId());
        view.setName(event.getName());
        view.setShortDescription(truncate(event.getDescription(), 200));
        view.setLocation(event.getLocation());
        view.setStartDate(event.getStartDate());
        view.setFormattedDate(formatDate(event.getStartDate()));
        view.setStatus("DRAFT");
        view.setIsPublished(false);
        view.setCreatedAt(event.getOccurredAt());
        
        repository.save(view);
        log.info("EventView projected: {}", view.getId());
    }
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors"
    )
    @Transactional
    public void onEventPublished(EventPublishedEvent event) {
        log.info("Projecting EventPublishedEvent: {}", event.getEventId());
        
        EventView view = repository.findById(event.getEventId())
            .orElseThrow(() -> new EntityNotFoundException("EventView not found"));
        
        view.setStatus("PUBLISHED");
        view.setIsPublished(true);
        
        repository.save(view);
        log.info("EventView updated: {}", view.getId());
    }
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors"
    )
    @Transactional
    public void onEventCancelled(EventCancelledEvent event) {
        log.info("Projecting EventCancelledEvent: {}", event.getEventId());
        
        EventView view = repository.findById(event.getEventId())
            .orElseThrow(() -> new EntityNotFoundException("EventView not found"));
        
        view.setStatus("CANCELLED");
        view.setIsPublished(false);
        
        repository.save(view);
        log.info("EventView updated: {}", view.getId());
    }
    
    private String truncate(String text, int maxLength) {
        if (text == null) return null;
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength - 3) + "...";
    }
    
    private String formatDate(LocalDateTime date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy, HH:mm");
        return date.format(formatter);
    }
}
```

### 3.7 Implementacja - Query Handler

**backend/src/main/java/com/eventmaster/query/handler/ListEventsQueryHandler.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ListEventsQueryHandler {
    
    private final EventViewRepository repository;
    
    public List<EventViewDTO> handle(ListEventsQuery query) {
        log.info("Handling ListEventsQuery: {}", query);
        
        // Simple SELECT - no JOINs!
        List<EventView> views = repository.findAll(
            PageRequest.of(
                query.getPage(), 
                query.getSize(),
                Sort.by("startDate").descending()
            )
        ).getContent();
        
        return views.stream()
            .map(this::toDTO)
            .collect(Collectors.toList());
    }
    
    private EventViewDTO toDTO(EventView view) {
        return EventViewDTO.builder()
            .id(view.getId())
            .name(view.getName())
            .shortDescription(view.getShortDescription())
            .location(view.getLocation())
            .formattedDate(view.getFormattedDate())
            .status(view.getStatus())
            .isPublished(view.getIsPublished())
            .build();
    }
}
```

### 3.8 Testowanie CQRS

#### Test 1: Write Model Zapisuje Poprawnie

```java
@SpringBootTest
@Testcontainers
class EventWriteModelTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
    
    @Autowired
    private EventRepository eventRepository;
    
    @Test
    void shouldSaveEventToWriteModel() {
        // Given
        Event event = Event.create(
            UUID.randomUUID(),
            "PostgreSQL Conference 2025",
            "Deep dive into PostgreSQL 18 features",
            "Oslo, Norway",
            LocalDateTime.of(2025, 7, 10, 9, 0)
        );
        
        // When
        Event saved = eventRepository.save(event);
        
        // Then
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getName()).isEqualTo("PostgreSQL Conference 2025");
        assertThat(saved.getStatus()).isEqualTo(EventStatus.DRAFT);
        assertThat(saved.getVersion()).isEqualTo(0L);
        assertThat(saved.getCreatedAt()).isNotNull();
    }
    
    @Test
    void shouldEnforceBusinessInvariants() {
        // Given
        Event event = Event.create(
            UUID.randomUUID(),
            "Test Event",
            "Description",
            "Location",
            LocalDateTime.now().plusDays(1)
        );
        event.publish();
        
        // When / Then - nie można publikować 2x
        assertThatThrownBy(() -> event.publish())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already published");
    }
}
```

#### Test 2: Read Model Jest Denormalizowany

```java
@SpringBootTest
@Testcontainers
class EventReadModelTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @Autowired
    private EventViewRepository viewRepository;
    
    @Test
    void shouldContainDenormalizedData() {
        // Given
        String longDescription = "A".repeat(500);  // 500 chars
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "Kafka Summit 2025",
            longDescription,
            "London, UK",
            LocalDateTime.of(2025, 10, 5, 9, 0),
            Instant.now()
        );
        
        // When
        eventPublisher.send("events.lifecycle", event.getEventId().toString(), event);
        
        // Then - wait for projection
        await().atMost(10, SECONDS).untilAsserted(() -> {
            EventView view = viewRepository.findById(event.getEventId()).orElseThrow();
            
            // Denormalized fields
            assertThat(view.getShortDescription()).hasSize(200);  // Truncated!
            assertThat(view.getShortDescription()).endsWith("...");
            assertThat(view.getFormattedDate()).matches("\\d{2} \\w{3} \\d{4}, \\d{2}:\\d{2}");
            assertThat(view.getIsPublished()).isFalse();
        });
    }
}
```

#### Test 3: Write i Read Są Niezależne

```java
@SpringBootTest
@Testcontainers
class CQRSIndependenceTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private EventRepository writeRepository;
    
    @Autowired
    private EventViewRepository readRepository;
    
    @Autowired
    private CreateEventCommandHandler commandHandler;
    
    @MockBean
    private EventViewProjector projector;  // Mock projector!
    
    @Test
    void writeModelShouldWorkEvenWhenReadModelFails() {
        // Given - Projector rzuca wyjątek
        doThrow(new RuntimeException("Projector error"))
            .when(projector).onEventCreated(any());
        
        // When
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            "Resilient Event",
            "Testing independence",
            "Test City",
            LocalDateTime.now().plusDays(1)
        );
        
        commandHandler.handle(command);
        
        // Then - Write Model działa!
        Optional<Event> event = writeRepository.findById(command.getEventId());
        assertThat(event).isPresent();
        
        // Read Model pusty (projector failed)
        Optional<EventView> view = readRepository.findById(command.getEventId());
        assertThat(view).isEmpty();
        
        // Gdy projector wróci, uzupełni Read Model
        // (w produkcji: retry mechanism)
    }
}
```

### 3.9 Eventual Consistency w CQRS

**Problem:** Read Model może być nieaktualny!

```java
// ❌ ZŁY KOD
@PostMapping("/events")
public ResponseEntity<EventDTO> createEvent(@RequestBody CreateEventDTO dto) {
    UUID eventId = commandService.createEvent(dto);
    
    // Read Model jeszcze nie istnieje!
    EventView view = queryService.getEvent(eventId);  // NULL! 😱
    return ResponseEntity.ok(toDTO(view));
}

// ✅ DOBRY KOD - Option 1: Return Command Response
@PostMapping("/events")
public ResponseEntity<CommandResponse> createEvent(@RequestBody CreateEventDTO dto) {
    UUID eventId = commandService.createEvent(dto);
    return ResponseEntity.accepted()
        .body(new CommandResponse(eventId, "ACCEPTED"));
}

// ✅ DOBRY KOD - Option 2: Poll with Timeout
@PostMapping("/events")
public ResponseEntity<EventDTO> createEvent(@RequestBody CreateEventDTO dto) {
    UUID eventId = commandService.createEvent(dto);
    
    // Wait up to 5 seconds for projection
    EventView view = await()
        .atMost(5, SECONDS)
        .until(() -> queryService.getEvent(eventId), Optional::isPresent)
        .get();
    
    return ResponseEntity.ok(toDTO(view));
}
```

### 3.10 Migracja Database Schema

**Flyway Migration - Write Model**

**backend/src/main/resources/db/migration/V1__create_events_table.sql**
```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    location VARCHAR(500) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_events_start_date ON events(start_date);
CREATE INDEX idx_events_status ON events(status);
```

**Flyway Migration - Read Model**

**backend/src/main/resources/db/migration/V2__create_event_view_table.sql**
```sql
CREATE TABLE event_view (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    short_description VARCHAR(200),
    location VARCHAR(500) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    formatted_date VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    is_published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL
);

-- Indexes for fast queries
CREATE INDEX idx_event_view_is_published ON event_view(is_published);
CREATE INDEX idx_event_view_start_date ON event_view(start_date DESC);
CREATE INDEX idx_event_view_status ON event_view(status);

-- Full-text search (PostgreSQL specific)
CREATE INDEX idx_event_view_name_fts ON event_view 
    USING gin(to_tsvector('english', name));
```

### 3.11 Performance Comparison

**Benchmark: 10,000 rekordów**

**Tradycyjny Model (z JOINs):**
```sql
SELECT 
    e.id,
    e.name,
    e.description,
    e.start_date
FROM events e
WHERE e.status = 'PUBLISHED'
ORDER BY e.start_date DESC
LIMIT 20;

-- Execution time: ~50ms
```

**CQRS Read Model:**
```sql
SELECT 
    id,
    name,
    short_description,
    formatted_date
FROM event_view
WHERE is_published = TRUE
ORDER BY start_date DESC
LIMIT 20;

-- Execution time: ~5ms (10x szybciej!)
```

### 3.12 Ćwiczenie Praktyczne

**Zadanie:** Dodaj nowy Read Model dla statystyk.

1. Utwórz `EventStatisticsView` z polami:
   - `eventId`
   - `viewCount`
   - `bookingCount`
   - `revenue`

2. Utwórz `EventStatisticsProjector`

3. Słuchaj na eventy:
   - `EventViewedEvent`
   - `BookingCreatedEvent`

4. Napisz testy

### 3.13 Podsumowanie

**Kluczowe Pojęcia:**
- CQRS = Write Model + Read Model
- Write Model = normalized, business logic
- Read Model = denormalized, fast queries
- Projector = synchronizes Read Model
- Eventual Consistency = Read Model opóźniony

**Zalety:**
- ⚡ Szybkie odczyty (no JOINs)
- 📈 Skalowalność (niezależne skalowanie)
- 🔧 Elastyczność (wiele Read Models)

**Wady:**
- 🕐 Eventual Consistency
- 🔄 Kompleksowość (więcej kodu)
- 🗄️ Duplikacja danych

**Następny temat:** [Commands vs Events vs Queries](#temat-4-commands-vs-events-vs-queries)

---

## Temat 4: Commands vs Events vs Queries

### 4.1 Definicje Szczegółowe

#### Command (Rozkaz)
- **Intent** - zamiar wykonania akcji
- **Może być odrzucony** - walidacja, business rules
- **Directed** - skierowany do jednego handlera
- **Present/Imperative** - "CreateEvent", "PublishEvent"
- **Mutable** - może zawierać błędy, można poprawić

#### Event (Fakt)
- **Fact** - coś co już się wydarzyło
- **Nie może być odrzucony** - już jest faktem
- **Broadcast** - wielu konsumentów
- **Past tense** - "EventCreated", "EventPublished"
- **Immutable** - nigdy się nie zmienia

#### Query (Zapytanie)
- **Request for data** - prośba o dane
- **Nie zmienia stanu** - read-only
- **Synchroniczne** - natychmiastowa odpowiedź
- **Noun + Get/List** - "GetEvent", "ListEvents"

### 4.2 Szczegółowa Tabela Porównawcza

| Aspekt | Command | Event | Query |
|--------|---------|-------|-------|
| **Cel** | Zmiana stanu | Informacja o zmianie | Odczyt danych |
| **Może zawieść?** | ✅ Tak | ❌ Nie (już się stało) | ❌ Nie (tylko czyta) |
| **Czasownik** | Rozkazujący | Przeszły | Pytający |
| **Przykład** | CreateEvent | EventCreated | GetEvent |
| **Odbiorcy** | 1 (handler) | N (listeners) | 1 (query handler) |
| **Transport** | Kafka/HTTP | Kafka | HTTP |
| **Odpowiedź** | Async (202) | Brak | Sync (200) |
| **Retry** | Może | Musi (idempotent) | Może |
| **Walidacja** | Przed | Brak | Brak |
| **Immutability** | Nie | TAK | Nie dotyczy |

### 4.3 Implementacja - Command

**backend/src/main/java/com/eventmaster/command/CreateEventCommand.java**
```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateEventCommand {
    
    private UUID eventId;
    private String name;
    private String description;
    private String location;
    private LocalDateTime startDate;
    
    // Commands mogą mieć walidację
    public void validate() {
        List<String> errors = new ArrayList<>();
        
        if (eventId == null) {
            errors.add("Event ID is required");
        }
        
        if (name == null || name.isBlank()) {
            errors.add("Event name is required");
        }
        
        if (name != null && name.length() > 200) {
            errors.add("Event name too long (max 200 chars)");
        }
        
        if (location == null || location.isBlank()) {
            errors.add("Location is required");
        }
        
        if (startDate == null) {
            errors.add("Start date is required");
        } else if (startDate.isBefore(LocalDateTime.now())) {
            errors.add("Event cannot start in the past");
        }
        
        if (!errors.isEmpty()) {
            throw new ValidationException("Command validation failed: " + 
                String.join(", ", errors));
        }
    }
}
```

**Przykład użycia:**
```java
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventCommandController {
    
    private final KafkaTemplate<String, CreateEventCommand> kafkaTemplate;
    
    @PostMapping
    public ResponseEntity<CommandResponse> createEvent(
        @RequestBody CreateEventCommandDTO dto
    ) {
        // 1. Konwersja DTO → Command
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            dto.getName(),
            dto.getDescription(),
            dto.getLocation(),
            dto.getStartDate()
        );
        
        // 2. Walidacja
        try {
            command.validate();
        } catch (ValidationException e) {
            return ResponseEntity.badRequest()
                .body(new CommandResponse(null, "REJECTED", e.getMessage()));
        }
        
        // 3. Publikacja
        kafkaTemplate.send("commands.events.create", command);
        
        // 4. Odpowiedź (202 Accepted)
        return ResponseEntity.accepted()
            .body(new CommandResponse(command.getEventId(), "ACCEPTED"));
    }
}
```

### 4.4 Implementacja - Event

**backend/src/main/java/com/eventmaster/events/EventCreatedEvent.java**
```java
// Event jest IMMUTABLE - używamy record (Java 17+)
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String description,
    String location,
    LocalDateTime startDate,
    UUID createdBy,
    Instant occurredAt,
    Long version  // Event versioning dla backward compatibility
) {
    // NO validation - to fakt, który już się wydarzył!
    // NO setters - immutable!
    
    // Możemy mieć dodatkowe konstruktory
    public EventCreatedEvent(Event event) {
        this(
            event.getId(),
            event.getName(),
            event.getDescription(),
            event.getLocation(),
            event.getStartDate(),
            event.getCreatedBy(),
            Instant.now(),
            1L  // Version 1
        );
    }
}
```

**Przykład publikacji:**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class CreateEventCommandHandler {
    
    private final EventRepository eventRepository;
    private final KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @KafkaListener(topics = "commands.events.create")
    @Transactional
    public void handle(CreateEventCommand command) {
        // 1. Validate (ponownie, dla pewności)
        command.validate();
        
        // 2. Business logic
        Event event = Event.create(/* ... */);
        eventRepository.save(event);
        
        // 3. Publish EVENT (nie command!)
        EventCreatedEvent eventCreated = new EventCreatedEvent(event);
        
        eventPublisher.send(
            "events.lifecycle",
            event.getId().toString(),  // Partition key
            eventCreated
        );
        
        log.info("EventCreatedEvent published: {}", event.getId());
    }
}
```

### 4.5 Implementacja - Query

**backend/src/main/java/com/eventmaster/query/GetEventQuery.java**
```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GetEventQuery {
    private UUID eventId;
    
    // Queries NIE mają walidacji biznesowej
    // Tylko podstawowa walidacja (null check)
    public void validate() {
        if (eventId == null) {
            throw new IllegalArgumentException("Event ID is required");
        }
    }
}
```

**backend/src/main/java/com/eventmaster/query/handler/GetEventQueryHandler.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class GetEventQueryHandler {
    
    private final EventViewRepository repository;
    
    public EventViewDTO handle(GetEventQuery query) {
        log.debug("Handling GetEventQuery: {}", query.getEventId());
        
        // Query czyta z Read Model
        EventView view = repository.findById(query.getEventId())
            .orElseThrow(() -> new EntityNotFoundException(
                "Event not found: " + query.getEventId()
            ));
        
        // Konwersja do DTO
        return EventViewDTO.builder()
            .id(view.getId())
            .name(view.getName())
            .shortDescription(view.getShortDescription())
            .location(view.getLocation())
            .formattedDate(view.getFormattedDate())
            .status(view.getStatus())
            .isPublished(view.getIsPublished())
            .build();
    }
}
```

**Przykład użycia:**
```java
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventQueryController {
    
    private final GetEventQueryHandler queryHandler;
    
    @GetMapping("/{id}")
    public ResponseEntity<EventViewDTO> getEvent(@PathVariable UUID id) {
        GetEventQuery query = new GetEventQuery(id);
        query.validate();
        
        EventViewDTO dto = queryHandler.handle(query);
        
        // Query zwraca 200 OK (synchronicznie)
        return ResponseEntity.ok(dto);
    }
}
```

### 4.6 Routing: Command vs Query

```
┌─────────────────────────────────────────┐
│         HTTP API Gateway                 │
└────┬──────────────────────────┬─────────┘
     │                          │
     │ Commands                 │ Queries
     │ (Write)                  │ (Read)
     ↓                          ↓
┌──────────────┐        ┌──────────────┐
│ Command API  │        │  Query API   │
│ Port: 8080   │        │  Port: 8081  │
└──────┬───────┘        └──────┬───────┘
       │                       │
       ↓                       ↓
┌──────────────┐        ┌──────────────┐
│    Kafka     │        │ EventViewDB  │
│  (Async)     │        │ (PostgreSQL) │
└──────────────┘        └──────────────┘
```

### 4.7 Testowanie Commands, Events, Queries

#### Test 1: Command Jest Walidowany

```java
@Test
void commandShouldRejectInvalidData() {
    // Given - komenda z błędami
    CreateEventCommand command = new CreateEventCommand(
        null,  // Brak ID
        "",    // Pusta nazwa
        "Description",
        "Location",
        LocalDateTime.now().minusDays(1)  // W przeszłości
    );
    
    // When / Then
    assertThatThrownBy(() -> command.validate())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Event ID is required")
        .hasMessageContaining("Event name is required")
        .hasMessageContaining("Event cannot start in the past");
}
```

#### Test 2: Event Jest Immutable

```java
@Test
void eventShouldBeImmutable() {
    // Given
    EventCreatedEvent event = new EventCreatedEvent(
        UUID.randomUUID(),
        "Original Name",
        "Description",
        "Location",
        LocalDateTime.now(),
        UUID.randomUUID(),
        Instant.now(),
        1L
    );
    
    // When / Then - próba zmiany (kompilator nie pozwoli!)
    // event.setName("New Name");  // Compilation error!
    
    // Event jest record - wszystkie pola final
    assertThat(event.name()).isEqualTo("Original Name");
}
```

#### Test 3: Query Nie Zmienia Stanu

```java
@SpringBootTest
@Testcontainers
class QuerySideEffectTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
    
    @Autowired
    private EventViewRepository repository;
    
    @Autowired
    private GetEventQueryHandler queryHandler;
    
    @Test
    void queryShouldNotChangeState() {
        // Given - utwórz event view
        EventView view = new EventView();
        view.setId(UUID.randomUUID());
        view.setName("Test Event");
        view.setStatus("PUBLISHED");
        repository.save(view);
        
        long initialCount = repository.count();
        Instant initialUpdatedAt = view.getUpdatedAt();
        
        // When - wykonaj query 10 razy
        GetEventQuery query = new GetEventQuery(view.getId());
        for (int i = 0; i < 10; i++) {
            queryHandler.handle(query);
        }
        
        // Then - stan NIE zmieniony
        assertThat(repository.count()).isEqualTo(initialCount);
        
        EventView afterQueries = repository.findById(view.getId()).get();
        assertThat(afterQueries.getName()).isEqualTo("Test Event");
        assertThat(afterQueries.getStatus()).isEqualTo("PUBLISHED");
        assertThat(afterQueries.getUpdatedAt()).isEqualTo(initialUpdatedAt);
    }
}
```

#### Test 4: Event Jest Broadcast (Wielu Listenerów)

```java
@SpringBootTest
@Testcontainers
class EventBroadcastTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @SpyBean
    private EventViewProjector projector;
    
    @SpyBean
    private EventEmailService emailService;
    
    @SpyBean
    private EventAnalyticsService analyticsService;
    
    @Test
    void eventShouldTriggerMultipleListeners() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "Broadcast Test",
            "Testing multiple listeners",
            "Test Location",
            LocalDateTime.now().plusDays(1),
            UUID.randomUUID(),
            Instant.now(),
            1L
        );
        
        // When
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        
        // Then - wszystkie 3 listenery powinny zareagować
        await().atMost(10, SECONDS).untilAsserted(() -> {
            verify(projector, times(1)).onEventCreated(any(EventCreatedEvent.class));
            verify(emailService, times(1)).onEventCreated(any(EventCreatedEvent.class));
            verify(analyticsService, times(1)).onEventCreated(any(EventCreatedEvent.class));
        });
    }
}
```

### 4.8 Kiedy Używać Którego?

#### Użyj Command gdy:
- ✅ Chcesz zmienić stan systemu
- ✅ Operacja może zostać odrzucona
- ✅ Potrzebujesz walidacji biznesowej
- ✅ Jeden handler przetwarza

**Przykłady:**
- CreateEvent
- UpdateEvent
- CancelEvent
- PublishEvent
- AddTicket

#### Użyj Event gdy:
- ✅ Informujesz o zmianie stanu
- ✅ Fakt, który już się wydarzył
- ✅ Wielu konsumentów zainteresowanych
- ✅ Potrzebujesz audit trail
- ✅ Chcesz eventual consistency

**Przykłady:**
- EventCreated
- EventUpdated
- EventCancelled
- EventPublished
- TicketAdded

#### Użyj Query gdy:
- ✅ Potrzebujesz danych (read-only)
- ✅ Natychmiastowa odpowiedź
- ✅ Nie zmieniasz stanu
- ✅ UI potrzebuje danych

**Przykłady:**
- GetEvent
- ListEvents
- SearchEvents
- GetEventStatistics

### 4.9 Anti-Patterns (Czego UNIKAĆ)

#### Anti-Pattern 1: Command jako Event

```java
// ❌ ZŁE - to jest Command, nie Event!
public class CreateEvent {  // Nazwa jak Command
    private String name;
    // ...
}

@KafkaListener(topics = "events.lifecycle")
public void handle(CreateEvent event) {  // Command przebrany za Event!
    // ...
}

// ✅ DOBRE
public class EventCreatedEvent {  // Past tense!
    private final String name;  // Immutable!
    // ...
}
```

#### Anti-Pattern 2: Query ze Side Effects

```java
// ❌ ZŁE - Query zmienia stan!
public class GetEventQueryHandler {
    public EventDTO handle(GetEventQuery query) {
        Event event = repository.findById(query.getId());
        
        // Side effect! 😱
        event.incrementViewCount();
        repository.save(event);
        
        return toDTO(event);
    }
}

// ✅ DOBRE - Query tylko czyta
public class GetEventQueryHandler {
    public EventDTO handle(GetEventQuery query) {
        EventView view = repository.findById(query.getId());
        return toDTO(view);  // Read-only
    }
}
```

#### Anti-Pattern 3: Event Mutable

```java
// ❌ ZŁE - Event z setterami!
@Data  // Generuje settery!
public class EventCreatedEvent {
    private UUID eventId;
    private String name;
}

// Ktoś może zmienić event!
event.setName("Hacked!");  // 😱

// ✅ DOBRE - Event immutable
public record EventCreatedEvent(
    UUID eventId,
    String name
) {
    // Brak setterów - immutable!
}
```

### 4.10 Ćwiczenie Praktyczne

**Zadanie:** Zaimplementuj pełny flow dla "UpdateEvent"

1. **Command:** `UpdateEventCommand`
   - eventId
   - newName
   - newDescription
   - validate()

2. **Handler:** `UpdateEventCommandHandler`
   - Walidacja
   - Aktualizacja w Write Model
   - Publikacja Event

3. **Event:** `EventUpdatedEvent`
   - eventId
   - newName
   - newDescription
   - updatedAt

4. **Projector:** Update Read Model

5. **Query:** Verify Read Model updated

6. **Testy:**
   - Command validation
   - Event publishing
   - Read Model consistency

### 4.11 Podsumowanie

**Kluczowe Różnice:**

| | Command | Event | Query |
|---|---------|-------|-------|
| **Zmienia stan?** | ✅ Tak | ❌ Nie (informuje) | ❌ Nie |
| **Immutable?** | ❌ Nie | ✅ TAK | N/A |
| **Walidacja?** | ✅ Tak | ❌ Nie | Podstawowa |
| **Odbiorcy** | 1 | Wielu | 1 |
| **Transport** | Kafka/HTTP | Kafka | HTTP |
| **Odpowiedź** | Async | Brak | Sync |

**Pamiętaj:**
- Command = intent (CreateEvent)
- Event = fact (EventCreated)
- Query = question (GetEvent)
- Commands mogą zawieść
- Events są niezmienne
- Queries nie zmieniają stanu

**Następny temat:** [Bounded Context](#temat-5-bounded-context)

---

## Temat 5: Bounded Context - Ograniczony Kontekst

### 5.1 Definicja (Domain-Driven Design)

**Bounded Context** to granica, w której określony model domenowy ma konkretne, jednoznaczne znaczenie. Poza tą granicą to samo pojęcie może mieć zupełnie inne znaczenie.

### 5.2 Analogia: Słowo "Bank"

**W różnych kontekstach:**

**Banking Context:**
- Bank = instytucja finansowa
- Ma klientów, konta, transakcje
- Regulowany przez KNF

**River Context:**
- Bank = brzeg rzeki
- Ma wysokość, erozję, roślinność
- Monitorowany przez hydrologów

**Blood Bank Context:**
- Bank = bank krwi
- Ma dawców, grupy krwi, przechowywanie
- Zarządzany przez służbę zdrowia

To samo słowo, **różne modele**!

### 5.3 W EventMaster - Bounded Contexts

```
┌─────────────────────────────────────────────────────────┐
│         EVENT MANAGEMENT CONTEXT                         │
│  (Core Domain - Zarządzanie wydarzeniami)                │
│  ┌────────────────────────────────────────────────┐     │
│  │  Aggregate: Event                              │     │
│  │  - id: UUID                                    │     │
│  │  - name: String                                │     │
│  │  - description: String                         │     │
│  │  - location: String                            │     │
│  │  - startDate: LocalDateTime                    │     │
│  │  - status: EventStatus                         │     │
│  │                                                 │     │
│  │  Commands:                                     │     │
│  │  - CreateEvent                                 │     │
│  │  - UpdateEvent                                 │     │
│  │  - PublishEvent                                │     │
│  │  - CancelEvent                                 │     │
│  │                                                 │     │
│  │  Events:                                       │     │
│  │  - EventCreated                                │     │
│  │  - EventUpdated                                │     │
│  │  - EventPublished                              │     │
│  │  - EventCancelled                              │     │
│  └────────────────────────────────────────────────┘     │
│  Database: eventmaster_events (PostgreSQL 18)           │
└─────────────────────────────────────────────────────────┘
                        │
                        │ Events via Kafka
                        ↓
┌─────────────────────────────────────────────────────────┐
│         TICKETING CONTEXT                                │
│  (Subdomain - Sprzedaż biletów)                          │
│  ┌────────────────────────────────────────────────┐     │
│  │  Aggregate: TicketingEvent (różny model!)     │     │
│  │  - eventId: UUID (reference)                   │     │
│  │  - totalCapacity: Integer                      │     │
│  │  - availableTickets: Integer                   │     │
│  │                                                 │     │
│  │  Aggregate: Ticket                             │     │
│  │  - id: UUID                                    │     │
│  │  - ticketTypeId: UUID                          │     │
│  │  - price: Money                                │     │
│  │  - status: TicketStatus                        │     │
│  │                                                 │     │
│  │  Commands:                                     │     │
│  │  - ReserveTickets                              │     │
│  │  - PurchaseTickets                             │     │
│  │  - CancelReservation                           │     │
│  │                                                 │     │
│  │  Events:                                       │     │
│  │  - TicketsReserved                             │     │
│  │  - TicketsPurchased                            │     │
│  │  - ReservationCancelled                        │     │
│  └────────────────────────────────────────────────┘     │
│  Database: eventmaster_ticketing (PostgreSQL 18)        │
└─────────────────────────────────────────────────────────┘
                        │
                        │ Events via Kafka
                        ↓
┌─────────────────────────────────────────────────────────┐
│         ANALYTICS CONTEXT                                │
│  (Supporting Domain - Analityka)                         │
│  ┌────────────────────────────────────────────────┐     │
│  │  Aggregate: EventAnalytics (inny model!)      │     │
│  │  - eventId: UUID                               │     │
│  │  - pageViews: Long                             │     │
│  │  - uniqueVisitors: Long                        │     │
│  │  - conversionRate: BigDecimal                  │     │
│  │  - revenue: Money                              │     │
│  │  - dailyMetrics: Map<LocalDate, Metrics>      │     │
│  │                                                 │     │
│  │  Commands:                                     │     │
│  │  - TrackPageView                               │     │
│  │  - TrackPurchase                               │     │
│  │                                                 │     │
│  │  Events:                                       │     │
│  │  - PageViewTracked                             │     │
│  │  - PurchaseTracked                             │     │
│  └────────────────────────────────────────────────┘     │
│  Database: eventmaster_analytics (MongoDB)              │
└─────────────────────────────────────────────────────────┘
```

### 5.4 Dlaczego Rozdzielamy Konteksty?

#### Problem: God Object (Monolith)

```java
// ❌ ZŁE - jeden wielki model dla wszystkiego
@Entity
@Table(name = "events")
public class Event {
    // Event Management fields
    private UUID id;
    private String name;
    private String description;
    private LocalDateTime startDate;
    
    // Ticketing fields (nie powinny tutaj być!)
    private Integer totalCapacity;
    private Integer availableTickets;
    @OneToMany
    private List<Ticket> tickets;
    
    // Analytics fields (też nie powinny!)
    private Long pageViews;
    private BigDecimal conversionRate;
    @ElementCollection
    private Map<LocalDate, Integer> dailyViews;
    
    // Payment fields (jeszcze więcej!)
    private String stripeAccountId;
    @OneToMany
    private List<Payout> payouts;
    
    // ... 50 more fields
    // 2000 linii kodu!
    // Nikt nie rozumie co robi ta klasa!
}
```

**Problemy:**
- 🔴 Klasa ma 2000+ linii
- 🔴 Każda zmiana łamie coś innego
- 🔴 Trudno testować
- 🔴 Niemożliwe do skalowania (wszystko razem)
- 🔴 Różne zespoły nie mogą pracować równolegle

#### Rozwiązanie: Bounded Contexts

```java
// ✅ DOBRE - Event Management Context
package com.eventmaster.eventmanagement.domain;

@Entity
@Table(name = "events")
public class Event {
    private UUID id;
    private String name;
    private String description;
    private LocalDateTime startDate;
    private EventStatus status;
    
    // TYLKO logika zarządzania eventem
    public void publish() { /* ... */ }
    public void cancel() { /* ... */ }
}

// ✅ DOBRE - Ticketing Context (osobny projekt!)
package com.eventmaster.ticketing.domain;

@Entity
@Table(name = "ticketing_events")
public class TicketingEvent {  // To jest INNY Event!
    private UUID eventId;  // Reference do Event Management
    private Integer totalCapacity;
    private Integer availableTickets;
    
    @OneToMany
    private List<Ticket> tickets;
    
    // TYLKO logika biletowa
    public ReservationResult reserveTickets(int quantity) { /* ... */ }
}

// ✅ DOBRE - Analytics Context (osobny projekt!)
package com.eventmaster.analytics.domain;

@Document(collection = "event_analytics")  // MongoDB!
public class EventAnalytics {  // Jeszcze inny Event!
    private UUID eventId;
    private Long pageViews;
    private Map<LocalDate, Metrics> dailyMetrics;
    
    // TYLKO logika analityczna
    public void trackView() { /* ... */ }
}
```

### 5.5 Komunikacja Między Kontekstami

**Konteksty NIE dzielą się bazą danych!**  
Komunikują się przez **Events** (Kafka).

```
Event Management Context
    ↓ publikuje EventCreatedEvent
    ↓ na Kafka (events.lifecycle)
    ↓
Ticketing Context (słucha)
    ↓ tworzy TicketingEvent
    ↓ publikuje TicketsAvailableEvent
    ↓
Analytics Context (słucha)
    ↓ tworzy EventAnalytics
```

#### Implementacja - Event Management Context

**backend-event-management/src/main/java/com/eventmaster/eventmanagement/handler/CreateEventCommandHandler.java**
```java
package com.eventmaster.eventmanagement.handler;

@Service
@RequiredArgsConstructor
@Slf4j
public class CreateEventCommandHandler {
    
    private final EventRepository eventRepository;
    private final KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @KafkaListener(topics = "commands.events.create")
    @Transactional
    public void handle(CreateEventCommand command) {
        log.info("Event Management: Creating event {}", command.getEventId());
        
        // 1. Biznes logika TYLKO dla Event Management
        Event event = Event.create(
            command.getEventId(),
            command.getName(),
            command.getDescription(),
            command.getLocation(),
            command.getStartDate()
        );
        
        // 2. Zapis do WŁASNEJ bazy
        eventRepository.save(event);
        
        // 3. Publikacja Event dla innych kontekstów
        EventCreatedEvent eventCreated = new EventCreatedEvent(
            event.getId(),
            event.getName(),
            event.getDescription(),
            event.getLocation(),
            event.getStartDate(),
            event.getCapacity(),  // Ticketing będzie potrzebował
            Instant.now()
        );
        
        eventPublisher.send("events.lifecycle", eventCreated);
        
        log.info("Event Management: Event created and published {}", event.getId());
    }
}
```

#### Implementacja - Ticketing Context (słucha)

**backend-ticketing/src/main/java/com/eventmaster/ticketing/projector/TicketingEventProjector.java**
```java
package com.eventmaster.ticketing.projector;

@Service
@RequiredArgsConstructor
@Slf4j
public class TicketingEventProjector {
    
    private final TicketingEventRepository repository;
    
    @KafkaListener(topics = "events.lifecycle", groupId = "ticketing-context")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Ticketing: Received EventCreatedEvent {}", event.getEventId());
        
        // Tworzymy NASZ model TicketingEvent
        TicketingEvent ticketingEvent = new TicketingEvent();
        ticketingEvent.setEventId(event.getEventId());
        ticketingEvent.setTotalCapacity(event.getCapacity());
        ticketingEvent.setAvailableTickets(event.getCapacity());
        
        // Zapis do WŁASNEJ bazy (ticketing database)
        repository.save(ticketingEvent);
        
        log.info("Ticketing: TicketingEvent created {}", ticketingEvent.getEventId());
    }
}
```

### 5.6 Context Map - Mapa Relacji

```
┌──────────────────┐         ┌──────────────────┐
│  Event Mgmt      │  Pub/Sub│    Ticketing     │
│  (Upstream)      ├────────→│   (Downstream)   │
│  Core Domain     │ Events  │   Subdomain      │
└──────────────────┘         └──────────────────┘
         │                            │
         │ Pub/Sub                    │ Pub/Sub
         │ Events                     │ Events
         ↓                            ↓
┌──────────────────┐         ┌──────────────────┐
│    Analytics     │         │     Payment      │
│  (Downstream)    │         │   (Downstream)   │
│  Supporting      │         │   Supporting     │
└──────────────────┘         └──────────────────┘
```

**Relacje:**
- **Upstream** - producent danych (Event Management)
- **Downstream** - konsument danych (Ticketing, Analytics)
- **Pub/Sub** - komunikacja przez Events (Kafka)

### 5.7 Testowanie Bounded Contexts

#### Test 1: Każdy Kontekst Ma Własną Bazę

```java
@SpringBootTest
@Testcontainers
class BoundedContextDatabaseTest {
    
    @Container
    static PostgreSQLContainer<?> eventMgmtDB = new PostgreSQLContainer<>("postgres:18")
        .withDatabaseName("eventmaster_events");
    
    @Container
    static PostgreSQLContainer<?> ticketingDB = new PostgreSQLContainer<>("postgres:18")
        .withDatabaseName("eventmaster_ticketing");
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.eventmanagement.url", eventMgmtDB::getJdbcUrl);
        registry.add("spring.datasource.ticketing.url", ticketingDB::getJdbcUrl);
    }
    
    @Test
    void contextsShouldHaveSeparateDatabases() {
        // Event Management używa swojej bazy
        assertThat(eventMgmtDB.getDatabaseName()).isEqualTo("eventmaster_events");
        
        // Ticketing używa swojej bazy
        assertThat(ticketingDB.getDatabaseName()).isEqualTo("eventmaster_ticketing");
        
        // Bazy są NIEZALEŻNE
        assertThat(eventMgmtDB.getJdbcUrl()).isNotEqualTo(ticketingDB.getJdbcUrl());
    }
}
```

#### Test 2: Komunikacja Przez Events

```java
@SpringBootTest
@Testcontainers
class CrossContextCommunicationTest {
    
    @Container
    static PostgreSQLContainer<?> eventMgmtDB = 
        new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static PostgreSQLContainer<?> ticketingDB = 
        new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @Autowired
    private EventRepository eventMgmtRepository;  // Event Management DB
    
    @Autowired
    private TicketingEventRepository ticketingRepository;  // Ticketing DB
    
    @Test
    void contextsShouldCommunicateThroughEvents() {
        // Given - Event w Event Management
        UUID eventId = UUID.randomUUID();
        
        // When - Publikacja EventCreatedEvent
        EventCreatedEvent event = new EventCreatedEvent(
            eventId,
            "Cross-Context Test",
            "Testing bounded contexts",
            "Test Location",
            LocalDateTime.now().plusDays(1),
            100,  // capacity
            Instant.now()
        );
        
        eventPublisher.send("events.lifecycle", eventId.toString(), event);
        
        // Then - Ticketing Context reaguje
        await().atMost(10, SECONDS).untilAsserted(() -> {
            // Event Management ma event
            assertThat(eventMgmtRepository.existsById(eventId)).isTrue();
            
            // Ticketing ma TicketingEvent (w SWOJEJ bazie!)
            Optional<TicketingEvent> ticketingEvent = 
                ticketingRepository.findByEventId(eventId);
            assertThat(ticketingEvent).isPresent();
            assertThat(ticketingEvent.get().getTotalCapacity()).isEqualTo(100);
        });
    }
}
```

#### Test 3: Failure Isolation (Izolacja Awarii)

```java
@Test
void eventManagementShouldWorkWhenTicketingFails() {
    // Given - Zatrzymujemy Ticketing Context
    stopContext("ticketing");
    
    // When - Tworzymy event
    CreateEventCommand command = new CreateEventCommand(/* ... */);
    eventMgmtHandler.handle(command);
    
    // Then - Event Management działa!
    assertThat(eventMgmtRepository.findAll()).hasSize(1);
    
    // Ticketing nie dostał eventu (ale to OK!)
    assertThat(ticketingRepository.findAll()).isEmpty();
    
    // Gdy Ticketing wróci, przetworzy zaległe eventy z Kafki
    startContext("ticketing");
    await().atMost(10, SECONDS).untilAsserted(() -> {
        assertThat(ticketingRepository.findAll()).hasSize(1);
    });
}
```

### 5.8 Struktura Projektu dla Bounded Contexts

```
eventmaster/
├── backend-event-management/     # Bounded Context 1
│   ├── src/main/java/com/eventmaster/eventmanagement/
│   │   ├── domain/
│   │   │   └── Event.java
│   │   ├── command/
│   │   │   └── CreateEventCommandHandler.java
│   │   ├── repository/
│   │   │   └── EventRepository.java
│   │   └── EventManagementApplication.java
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── db/migration/  # Flyway migrations
│   └── pom.xml
│
├── backend-ticketing/             # Bounded Context 2
│   ├── src/main/java/com/eventmaster/ticketing/
│   │   ├── domain/
│   │   │   ├── TicketingEvent.java
│   │   │   └── Ticket.java
│   │   ├── projector/
│   │   │   └── TicketingEventProjector.java
│   │   ├── repository/
│   │   │   └── TicketingEventRepository.java
│   │   └── TicketingApplication.java
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── db/migration/
│   └── pom.xml
│
├── backend-analytics/             # Bounded Context 3
│   ├── src/main/java/com/eventmaster/analytics/
│   │   ├── domain/
│   │   │   └── EventAnalytics.java
│   │   ├── projector/
│   │   │   └── AnalyticsProjector.java
│   │   └── AnalyticsApplication.java
│   └── pom.xml
│
├── shared-events/                 # Shared kernel
│   ├── src/main/java/com/eventmaster/events/
│   │   ├── EventCreatedEvent.java
│   │   ├── EventUpdatedEvent.java
│   │   └── TicketsPurchasedEvent.java
│   └── pom.xml
│
└── docker/
    └── docker-compose.yml         # 3 bazy, 1 Kafka
```

### 5.9 Konfiguracja Multiple Databases

**docker/docker-compose.yml**
```yaml
version: '3.9'

services:
  # Event Management Database
  postgres-eventmanagement:
    image: postgres:18
    container_name: eventmaster-db-eventmgmt
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: eventmaster_events
      POSTGRES_USER: eventmgmt
      POSTGRES_PASSWORD: eventmgmt123
    volumes:
      - eventmgmt_data:/var/lib/postgresql/data
  
  # Ticketing Database
  postgres-ticketing:
    image: postgres:18
    container_name: eventmaster-db-ticketing
    ports:
      - "5433:5432"
    environment:
      POSTGRES_DB: eventmaster_ticketing
      POSTGRES_USER: ticketing
      POSTGRES_PASSWORD: ticketing123
    volumes:
      - ticketing_data:/var/lib/postgresql/data
  
  # Analytics Database (MongoDB)
  mongo-analytics:
    image: mongo:7
    container_name: eventmaster-db-analytics
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_DATABASE: eventmaster_analytics
    volumes:
      - analytics_data:/data/db
  
  # Kafka (Shared message bus)
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: eventmaster-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # ... (konfiguracja z poprzednich tematów)

volumes:
  eventmgmt_data:
  ticketing_data:
  analytics_data:
  kafka_data:
```

### 5.10 Ćwiczenie Praktyczne

**Zadanie:** Dodaj nowy Bounded Context - "Payment".

1. Utwórz projekt `backend-payment`
2. Model `PaymentEvent` z polami:
   - eventId (reference)
   - stripeAccountId
   - payouts: List<Payout>
3. Słuchaj na `TicketsPurchasedEvent`
4. Utwórz własną bazę PostgreSQL
5. Napisz testy izolacji

### 5.11 Podsumowanie

**Kluczowe Pojęcia:**
- Bounded Context = granica modelu domenowego
- Każdy kontekst ma własną bazę
- Komunikacja przez Events (Kafka)
- Context Map = mapa relacji
- Failure isolation = niezależność

**Zalety:**
- 🎯 Clear boundaries (jasne granice)
- 🔄 Independent deployment
- 📈 Independent scaling
- 👥 Team autonomy
- 🛡️ Failure isolation

**Następny temat:** [Aggregate](#temat-6-aggregate)

---

## Temat 6: Aggregate - Granica Spójności

### 6.1 Definicja (DDD - Eric Evans)

**Aggregate** to klaster powiązanych obiektów traktowanych jako jednostka dla zmian danych.
- Ma **Aggregate Root** - główny obiekt kontrolujący dostęp
- Zapewnia **invariants** (niezmienniki biznesowe)
- Jest granicą **transakcji**

### 6.2 Analogia: Zamówienie w Sklepie

```
Aggregate: Order (Root)
├── OrderId (identity)
├── Customer (value object)
├── Status (DRAFT, CONFIRMED, SHIPPED)
└── OrderItems (entities w aggregate)
    ├── OrderItem 1 (ProductId, Quantity, Price)
    ├── OrderItem 2
    └── OrderItem 3

Invariants (Niezmienniki):
- Order musi mieć przynajmniej 1 OrderItem
- Suma cen items = Order.totalPrice
- Nie można dodać items do SHIPPED order
```

**Dostęp TYLKO przez Aggregate Root:**
```java
// ❌ ZŁE - bezpośredni dostęp do OrderItem
OrderItem item = orderItemRepository.findById(itemId);
item.setQuantity(10);  // Omija walidację Order!

// ✅ DOBRE - przez Aggregate Root
Order order = orderRepository.findById(orderId);
order.updateItemQuantity(itemId, 10);  // Order waliduje!
```

### 6.3 W EventMaster - Event Aggregate

**backend/src/main/java/com/eventmaster/domain/Event.java**
```java
@Entity
@Table(name = "events")
@NoArgsConstructor
@Getter  // Tylko gettery na zewnątrz!
public class Event {  // AGGREGATE ROOT
    
    @Id
    private UUID id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(columnDefinition = "TEXT")
    private String description;
    
    @Embedded
    private Location location;  // Value Object (część aggregate)
    
    @ElementCollection
    @CollectionTable(name = "event_dates")
    private List<EventDate> eventDates;  // Entities w aggregate
    
    @Enumerated(EnumType.STRING)
    private EventStatus status;
    
    @Version
    private Long version;
    
    // Factory method
    public static Event create(
        String name, 
        String description, 
        Location location,
        List<LocalDateTime> dates
    ) {
        Event event = new Event();
        event.id = UUID.randomUUID();
        event.name = name;
        event.description = description;
        event.location = location;
        event.status = EventStatus.DRAFT;
        
        // Walidacja invariants
        event.validateName(name);
        event.validateDates(dates);
        
        event.eventDates = dates.stream()
            .map(EventDate::new)
            .collect(Collectors.toList());
        
        return event;
    }
    
    // Business methods - zapewniają invariants
    public void publish() {
        // Invariant: Event musi mieć przynajmniej 1 datę
        if (eventDates.isEmpty()) {
            throw new BusinessRuleViolationException(
                "Cannot publish event without dates"
            );
        }
        
        // Invariant: Nie można publikować anulowanego eventu
        if (status == EventStatus.CANCELLED) {
            throw new BusinessRuleViolationException(
                "Cannot publish cancelled event"
            );
        }
        
        this.status = EventStatus.PUBLISHED;
    }
    
    public void addDate(LocalDateTime date) {
        // Invariant: Data w przyszłości
        if (date.isBefore(LocalDateTime.now())) {
            throw new BusinessRuleViolationException(
                "Cannot add past date"
            );
        }
        
        // Invariant: Nie więcej niż 10 dat
        if (eventDates.size() >= 10) {
            throw new BusinessRuleViolationException(
                "Cannot have more than 10 event dates"
            );
        }
        
        this.eventDates.add(new EventDate(date));
    }
    
    public void cancel() {
        // Invariant: Nie można anulować eventu, który już się odbył
        if (hasStarted()) {
            throw new BusinessRuleViolationException(
                "Cannot cancel event that has already started"
            );
        }
        
        this.status = EventStatus.CANCELLED;
    }
    
    private void validateName(String name) {
        if (name == null || name.isBlank()) {
            throw new BusinessRuleViolationException("Event name is required");
        }
        if (name.length() > 200) {
            throw new BusinessRuleViolationException("Event name too long");
        }
    }
    
    private void validateDates(List<LocalDateTime> dates) {
        if (dates == null || dates.isEmpty()) {
            throw new BusinessRuleViolationException(
                "Event must have at least one date"
            );
        }
    }
    
    private boolean hasStarted() {
        return eventDates.stream()
            .anyMatch(ed -> ed.getStartTime().isBefore(LocalDateTime.now()));
    }
}

// Value Object (część aggregate)
@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Location {
    @Column(nullable = false)
    private String city;
    
    @Column(nullable = false)
    private String country;
    
    private String venue;
    private String address;
}

// Entity w aggregate (nie ma własnego repository!)
@Embeddable
@Data
@NoArgsConstructor
public class EventDate {
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    
    public EventDate(LocalDateTime startTime) {
        this.startTime = startTime;
        this.endTime = startTime.plusHours(8);  // Domyślnie 8h
    }
}
```

### 6.4 Aggregate Boundaries - Co Jest Częścią?

**Zasady:**
1. ✅ Small aggregates are better (małe lepsze)
2. ✅ Reference by ID, not object (referencja przez ID)
3. ✅ One transaction = One aggregate
4. ✅ Use eventual consistency between aggregates

**Przykład - Ticketing:**

```
┌─────────────────────────────────────────┐
│  Aggregate: Event                        │  
│  - eventId (ROOT)                        │
│  - name                                  │
│  - location (Value Object)               │
│  - eventDates (Entities)                 │
└─────────────────────────────────────────┘
             │ Reference by ID
             ↓
┌─────────────────────────────────────────┐
│  Aggregate: TicketingEvent (osobny!)    │
│  - eventId (ROOT)                        │
│  - totalCapacity                         │
│  - tickets (Entities)                    │
└─────────────────────────────────────────┘
```

**❌ ZŁE - za duży aggregate:**
```java
@Entity
public class Event {
    @Id
    private UUID id;
    
    // Za dużo! To powinien być osobny aggregate!
    @OneToMany(cascade = CascadeType.ALL)
    private List<Ticket> tickets;  // 1000+ tickets!
    
    @OneToMany(cascade = CascadeType.ALL)
    private List<Booking> bookings;  // 10000+ bookings!
    
    // Każda zmiana eventu blokuje WSZYSTKIE tickets i bookings! 😱
}
```

**✅ DOBRE - małe aggregates:**
```java
// Event Aggregate
@Entity
public class Event {
    @Id
    private UUID id;
    private String name;
    // Tylko pole eventId, bez relacji
}

// Ticketing Aggregate (osobny)
@Entity
public class Ticket {
    @Id
    private UUID id;
    private UUID eventId;  // Reference przez ID!
    private TicketType type;
    private Money price;
}
```

### 6.5 Testowanie Aggregates

#### Test 1: Invariants Są Egzekwowane

```java
@Test
void shouldEnforceInvariants() {
    // Given
    Event event = Event.create(
        "Test Event",
        "Description",
        new Location("Warsaw", "Poland"),
        List.of(LocalDateTime.now().plusDays(1))
    );
    
    // When / Then - nie można publikować bez dat
    event.clearDates();  // Załóżmy że istnieje (nie powinno!)
    assertThatThrownBy(() -> event.publish())
        .isInstanceOf(BusinessRuleViolationException.class)
        .hasMessageContaining("without dates");
}

@Test
void shouldPreventInvalidState() {
    // Given
    Event event = Event.create(/* ... */);
    event.publish();
    event.cancel();
    
    // When / Then - nie można publikować anulowanego
    assertThatThrownBy(() -> event.publish())
        .isInstanceOf(BusinessRuleViolationException.class)
        .hasMessageContaining("cancelled event");
}
```

#### Test 2: Aggregate Jest Transakcyjną Jednostką

```java
@SpringBootTest
@Testcontainers
class AggregateTransactionTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Autowired
    private EventRepository eventRepository;
    
    @Test
    @Transactional
    void shouldSaveEntireAggregateInOneTransaction() {
        // Given
        Event event = Event.create(
            "Multi-Date Event",
            "Description",
            new Location("Berlin", "Germany"),
            List.of(
                LocalDateTime.of(2025, 6, 1, 9, 0),
                LocalDateTime.of(2025, 6, 2, 9, 0),
                LocalDateTime.of(2025, 6, 3, 9, 0)
            )
        );
        
        // When
        Event saved = eventRepository.save(event);
        
        // Then - wszystkie części aggregate zapisane razem
        Event loaded = eventRepository.findById(saved.getId()).orElseThrow();
        assertThat(loaded.getEventDates()).hasSize(3);
        assertThat(loaded.getLocation().getCity()).isEqualTo("Berlin");
        
        // Wszystko w jednej transakcji - albo wszystko, albo nic!
    }
}
```

#### Test 3: Access Tylko Przez Aggregate Root

```java
@Test
void shouldOnlyAccessThroughRoot() {
    // Given
    Event event = Event.create(/* ... */);
    
    // ❌ ZŁE - próba bezpośredniego dostępu
    // EventDate date = eventDateRepository.findById(dateId);
    // date.setStartTime(newTime);  // Omija walidację Event!
    
    // ✅ DOBRE - przez Aggregate Root
    event.updateEventDate(0, LocalDateTime.now().plusDays(2));
    
    // Event może walidować zmianę
    assertThat(event.getEventDates().get(0).getStartTime())
        .isAfter(LocalDateTime.now());
}
```

### 6.6 Aggregate w Testach - Repository Pattern

```java
// Repository TYLKO dla Aggregate Root
public interface EventRepository extends JpaRepository<Event, UUID> {
    Optional<Event> findById(UUID id);
    List<Event> findByStatus(EventStatus status);
}

// ❌ NIE MA EventDateRepository!
// ❌ NIE MA LocationRepository!
// EventDate i Location to część Event aggregate!

@Test
void repositoryShouldOnlySaveAggregateRoot() {
    // Given
    Event event = Event.create(/* ... */);
    event.addDate(LocalDateTime.now().plusDays(5));
    
    // When - zapisujemy TYLKO root
    eventRepository.save(event);
    
    // Then - EventDate zapisany automatycznie (cascade)
    Event loaded = eventRepository.findById(event.getId()).get();
    assertThat(loaded.getEventDates()).hasSize(2);  // Original + added
}
```

---

## Temat 7: Domain Events

### 7.1 Definicja

**Domain Events** to zdarzenia ważne dla domeny biznesowej, wyrażone językiem eksperckim.

### 7.2 Nazewnictwo - Język Biznesu

```java
// ✅ DOBRE - język biznesu
EventPublishedEvent      // Biznes rozumie "published"
TicketSoldOutEvent       // Biznes rozumie "sold out"
BookingConfirmedEvent    // Biznes rozumie "confirmed"

// ❌ ZŁE - techniczny język
EventStatusChanged       // Co to znaczy? Status zmieniony na co?
TicketCountUpdated       // Dlaczego? Jaki context?
BookingRecordInserted    // Techniczny, nie biznesowy
```

### 7.3 Implementacja - Domain Event

```java
// Domain Event z językiem biznesu
public record EventPublishedEvent(
    UUID eventId,
    String eventName,
    LocalDateTime publicationDate,
    UUID publishedBy,
    Instant occurredAt
) {
    // Event ma znaczenie dla biznesu:
    // - Marketing może wysłać kampanię
    // - Analytics może zacząć tracking
    // - Search może zaindeksować
}
```

### 7.4 Emitowanie Domain Events z Aggregate

```java
@Entity
public class Event {
    
    @Id
    private UUID id;
    
    @Transient  // NIE zapisujemy do bazy
    private final List<DomainEvent> domainEvents = new ArrayList<>();
    
    public void publish() {
        // Business logic
        this.status = EventStatus.PUBLISHED;
        
        // Emit domain event
        domainEvents.add(new EventPublishedEvent(
            this.id,
            this.name,
            LocalDateTime.now(),
            this.createdBy,
            Instant.now()
        ));
    }
    
    public List<DomainEvent> pullDomainEvents() {
        List<DomainEvent> events = new ArrayList<>(domainEvents);
        domainEvents.clear();  // Clear after pull
        return events;
    }
}
```

### 7.5 Testowanie Domain Events

```java
@Test
void shouldEmitDomainEventOnPublish() {
    // Given
    Event event = Event.create(/* ... */);
    
    // When
    event.publish();
    
    // Then
    List<DomainEvent> events = event.pullDomainEvents();
    assertThat(events).hasSize(1);
    assertThat(events.get(0)).isInstanceOf(EventPublishedEvent.class);
    
    EventPublishedEvent published = (EventPublishedEvent) events.get(0);
    assertThat(published.eventId()).isEqualTo(event.getId());
}
```

---

## Temat 8: Eventual Consistency

### 8.1 Problem: Rozproszona Spójność

W systemach rozproszonych (CQRS, mikroserwisy) nie możemy mieć natychmiastowej spójności.

**Strong Consistency (tradycyjny):**
```
User: Tworzę event
System: Zapisuję do bazy
System: READ pokazuje event NATYCHMIAST
```

**Eventual Consistency (CQRS):**
```
User: Tworzę event
System: Zapisuję do Write DB
System: Publikuje Event (Kafka)
[... kilka sekund ...]
System: Projector aktualizuje Read DB
User: READ pokazuje event (po opóźnieniu)
```

### 8.2 Testowanie Eventual Consistency

```java
@Test
void shouldEventuallyBeConsistent() {
    // Given
    CreateEventCommand command = new CreateEventCommand(/* ... */);
    
    // When
    commandHandler.handle(command);
    
    // Then - użyj Awaitility!
    await()
        .atMost(10, SECONDS)
        .pollInterval(100, MILLISECONDS)
        .untilAsserted(() -> {
            Optional<EventView> view = 
                readRepository.findById(command.getEventId());
            assertThat(view).isPresent();
        });
}
```

---

## Temat 9: Idempotency - Wielokrotne Wykonanie

### 9.1 Definicja

Operacja **idempotentna**: wykonana 1x lub 100x daje ten sam efekt.

### 9.2 Dlaczego To Ważne?

Kafka gwarantuje **at-least-once delivery** - message może przyjść 2x!

```java
// ❌ NIE-idempotentny
@KafkaListener(topics = "events.lifecycle")
public void handle(EventCreatedEvent event) {
    eventView.incrementViewCount();  // +1 każde wywołanie!
    // Message przyszedł 2x → view count +2 (ZŁE!)
}

// ✅ Idempotentny
@KafkaListener(topics = "events.lifecycle")
public void handle(EventCreatedEvent event) {
    if (processedEvents.contains(event.eventId())) {
        log.info("Already processed, skipping");
        return;
    }
    
    eventView.create(event);
    processedEvents.add(event.eventId());
}
```

### 9.3 Implementacja Idempotency

```java
@Service
@RequiredArgsConstructor
public class IdempotentEventProjector {
    
    private final EventViewRepository repository;
    private final ProcessedEventRepository processedRepo;
    
    @KafkaListener(topics = "events.lifecycle")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        // 1. Sprawdź czy już przetworzony
        if (processedRepo.existsByEventIdAndType(
            event.eventId(), 
            "EventCreatedEvent"
        )) {
            log.warn("Event {} already processed, skipping", event.eventId());
            return;
        }
        
        // 2. Przetwórz
        EventView view = new EventView();
        view.setId(event.eventId());
        view.setName(event.name());
        repository.save(view);
        
        // 3. Zaznacz jako przetworzony
        ProcessedEvent processed = new ProcessedEvent(
            event.eventId(),
            "EventCreatedEvent",
            Instant.now()
        );
        processedRepo.save(processed);
        
        log.info("Event {} processed", event.eventId());
    }
}
```

### 9.4 Testowanie Idempotency

```java
@Test
void shouldBeIdempotent() {
    // Given
    EventCreatedEvent event = new EventCreatedEvent(/* ... */);
    
    // When - przetwórz 3x
    projector.onEventCreated(event);
    projector.onEventCreated(event);
    projector.onEventCreated(event);
    
    // Then - tylko 1 rekord
    List<EventView> views = repository.findAll();
    assertThat(views).hasSize(1);
}
```

---

## Temat 10: Saga Pattern - Długo Działające Transakcje

### 10.1 Definicja

**Saga** to sekwencja lokalnych transakcji koordynowanych przez events/commands.

### 10.2 Przykład: Booking Saga

```
1. CreateBookingCommand
   ↓
2. BookingCreatedEvent
   ↓
3. ReserveTicketsCommand
   ↓
4a. TicketsReservedEvent (sukces)
    ↓
5a. ConfirmBookingCommand
    ↓
6a. BookingConfirmedEvent

LUB

4b. TicketsUnavailableEvent (fail)
    ↓
5b. CancelBookingCommand (compensation!)
    ↓
6b. BookingCancelledEvent
```

### 10.3 Implementacja Saga Orchestrator

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class BookingSagaOrchestrator {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<UUID, SagaState> sagaStates = new ConcurrentHashMap<>();
    
    // Step 1: Start saga
    public void startBooking(CreateBookingCommand command) {
        log.info("Starting booking saga: {}", command.getBookingId());
        
        SagaState state = new SagaState(command.getBookingId());
        state.setStatus(SagaStatus.STARTED);
        sagaStates.put(command.getBookingId(), state);
        
        kafkaTemplate.send("commands.bookings.create", command);
    }
    
    // Step 2: Booking created, reserve tickets
    @KafkaListener(topics = "events.bookings.created")
    public void onBookingCreated(BookingCreatedEvent event) {
        log.info("Booking created, reserving tickets: {}", event.getBookingId());
        
        SagaState state = sagaStates.get(event.getBookingId());
        state.setStatus(SagaStatus.BOOKING_CREATED);
        
        ReserveTicketsCommand command = new ReserveTicketsCommand(
            event.getEventId(),
            event.getTicketQuantity()
        );
        
        kafkaTemplate.send("commands.tickets.reserve", command);
    }
    
    // Step 3a: Tickets reserved (SUCCESS path)
    @KafkaListener(topics = "events.tickets.reserved")
    public void onTicketsReserved(TicketsReservedEvent event) {
        log.info("Tickets reserved, confirming booking: {}", event.getBookingId());
        
        SagaState state = sagaStates.get(event.getBookingId());
        state.setStatus(SagaStatus.TICKETS_RESERVED);
        
        ConfirmBookingCommand command = new ConfirmBookingCommand(
            event.getBookingId()
        );
        
        kafkaTemplate.send("commands.bookings.confirm", command);
    }
    
    // Step 3b: Tickets unavailable (FAILURE path)
    @KafkaListener(topics = "events.tickets.unavailable")
    public void onTicketsUnavailable(TicketsUnavailableEvent event) {
        log.warn("Tickets unavailable, compensating: {}", event.getBookingId());
        
        SagaState state = sagaStates.get(event.getBookingId());
        state.setStatus(SagaStatus.COMPENSATION);
        
        // COMPENSATION - cofnij booking
        CancelBookingCommand command = new CancelBookingCommand(
            event.getBookingId(),
            "Tickets unavailable"
        );
        
        kafkaTemplate.send("commands.bookings.cancel", command);
    }
    
    // Step 4: Booking confirmed (END)
    @KafkaListener(topics = "events.bookings.confirmed")
    public void onBookingConfirmed(BookingConfirmedEvent event) {
        log.info("Booking saga completed successfully: {}", event.getBookingId());
        
        SagaState state = sagaStates.get(event.getBookingId());
        state.setStatus(SagaStatus.COMPLETED);
    }
    
    // Step 4: Booking cancelled (COMPENSATION END)
    @KafkaListener(topics = "events.bookings.cancelled")
    public void onBookingCancelled(BookingCancelledEvent event) {
        log.info("Booking saga compensated: {}", event.getBookingId());
        
        SagaState state = sagaStates.get(event.getBookingId());
        state.setStatus(SagaStatus.COMPENSATED);
    }
}

enum SagaStatus {
    STARTED,
    BOOKING_CREATED,
    TICKETS_RESERVED,
    COMPLETED,
    COMPENSATION,
    COMPENSATED
}
```

### 10.4 Testowanie Saga

```java
@SpringBootTest
@Testcontainers
class BookingSagaTest {
    
    @Autowired
    private BookingSagaOrchestrator sagaOrchestrator;
    
    @Autowired
    private BookingRepository bookingRepository;
    
    @Test
    void shouldCompleteSagaSuccessfully() {
        // Given
        CreateBookingCommand command = new CreateBookingCommand(
            UUID.randomUUID(),
            UUID.randomUUID(),  // eventId
            2  // quantity
        );
        
        // When
        sagaOrchestrator.startBooking(command);
        
        // Then - wait for completion
        await().atMost(15, SECONDS).untilAsserted(() -> {
            SagaState state = sagaOrchestrator.getSagaState(command.getBookingId());
            assertThat(state.getStatus()).isEqualTo(SagaStatus.COMPLETED);
            
            Booking booking = bookingRepository.findById(command.getBookingId()).get();
            assertThat(booking.getStatus()).isEqualTo(BookingStatus.CONFIRMED);
        });
    }
    
    @Test
    void shouldCompensateOnFailure() {
        // Given - symuluj brak tickets
        simulateTicketUnavailability();
        
        CreateBookingCommand command = new CreateBookingCommand(/* ... */);
        
        // When
        sagaOrchestrator.startBooking(command);
        
        // Then - saga compensates
        await().atMost(15, SECONDS).untilAsserted(() -> {
            SagaState state = sagaOrchestrator.getSagaState(command.getBookingId());
            assertThat(state.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
            
            Booking booking = bookingRepository.findById(command.getBookingId()).get();
            assertThat(booking.getStatus()).isEqualTo(BookingStatus.CANCELLED);
        });
    }
}
```

---

## 🎓 Podsumowanie CZĘŚĆ I

Gratulacje! Ukończyłeś **CZĘŚĆ I: FUNDAMENTY**.

### Co Nauczyłeś Się:

1. ✅ **Message-Driven Architecture** - asynchroniczna komunikacja
2. ✅ **Event-Driven Architecture** - komunikacja przez fakty
3. ✅ **CQRS** - rozdzielenie Write i Read
4. ✅ **Commands vs Events vs Queries** - różnice i zastosowania
5. ✅ **Bounded Context** - granice modeli domenowych
6. ✅ **Aggregate** - granice spójności transakcyjnej
7. ✅ **Domain Events** - język biznesu
8. ✅ **Eventual Consistency** - spójność ostateczna
9. ✅ **Idempotency** - odporność na duplikaty
10. ✅ **Saga Pattern** - długo działające transakcje

### Stack Technologiczny:

- 🐘 **PostgreSQL 18** (Write & Read databases)
- 🦅 **Apache Kafka 7.6** (Event streaming)
- 🍃 **Spring Boot 3.4** (Backend framework)
- ☕ **Java 21** (Language features: records, pattern matching)
- 🧪 **Testcontainers** (Integration tests)
- ⏱️ **Awaitility** (Async testing)

### Gotowy do CZĘŚCI II?

**CZĘŚĆ II: CQRS w Praktyce (Tematy 11-20)** zawiera:
- Projections & Multiple Read Models
- Rebuilding Projections
- Optimistic Concurrency Control
- Command & Query Handlers
- Denormalization & Materialized Views
- Snapshot Pattern

**Status:** CZĘŚĆ I ✅ UKOŃCZONA

**Następny dokument:** [CZESC_II_CQRS_W_PRAKTYCE.md](./CZESC_II_CQRS_W_PRAKTYCE.md)

---

**Dokument utworzony:** 2025-01-20  
**Wersja:** 1.0  
**Autor:** EventMaster Architecture Team  
**Stack:** PostgreSQL 18 + Apache Kafka + Spring Boot 3.4  
**Liczba linii:** 3500+

