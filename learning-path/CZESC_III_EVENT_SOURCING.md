# 🎓 CZĘŚĆ III: Event Sourcing (Tematy 21-30)

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Apache Kafka + Spring Boot 3.4 + Nuxt 3  
**Cel:** Zaawansowane wzorce Event Sourcing w praktyce  
**Czas nauki:** 3-4 tygodnie  
**Wymagania:** Ukończone [CZĘŚĆ I](./CZESC_I_FUNDAMENTY.md) i [CZĘŚĆ II](./CZESC_II_CQRS_W_PRAKTYCE.md)

---

## 📋 Spis Treści - CZĘŚĆ III

21. [Event Sourcing - Wprowadzenie](#temat-21-event-sourcing-wprowadzenie)
22. [Event Store - Implementation](#temat-22-event-store-implementation)
23. [Event Versioning](#temat-23-event-versioning)
24. [Upcasting Events](#temat-24-upcasting-events)
25. [Temporal Queries](#temat-25-temporal-queries)
26. [Event Store Optimization](#temat-26-event-store-optimization)
27. [GDPR & Event Sourcing](#temat-27-gdpr-event-sourcing)
28. [Event Compaction](#temat-28-event-compaction)
29. [Debugging with Events](#temat-29-debugging-with-events)
30. [Hybrid Approach](#temat-30-hybrid-approach)

---

## 🎯 Cele Nauki - CZĘŚĆ III

Po ukończeniu Części III będziesz:
- ✅ Implementować pełny Event Store
- ✅ Zarządzać wersjami events (versioning)
- ✅ Wykonywać temporal queries ("jak wyglądało 3 miesiące temu?")
- ✅ Optymalizować Event Store
- ✅ Obsługiwać GDPR w Event Sourcing
- ✅ Debugować aplikacje używając Event Store
- ✅ Łączyć Event Sourcing ze State-based persistence

---

## Temat 21: Event Sourcing - Wprowadzenie

### 21.1 Definicja

**Event Sourcing** to wzorzec persystencji, w którym zapisujemy **wszystkie zmiany stanu** jako sekwencję zdarzeń, zamiast zapisywać tylko obecny stan.

### 21.2 Analogia: System Bankowy

**Tradycyjne podejście (State-based):**
```
Konto nr 123456
Saldo: 1,000 PLN
```

**Problem:** Nie wiesz JAK doszło do tego salda!

**Event Sourcing:**
```
Event 1: KontoUtworzoneEvent        (+0 PLN)        2024-01-01 10:00
Event 2: WpłataDokonanaEvent        (+500 PLN)      2024-01-05 14:30
Event 3: WypłataDokonanaEvent       (-200 PLN)      2024-01-10 11:15
Event 4: WpłataDokonanaEvent        (+700 PLN)      2024-01-15 16:45
─────────────────────────────────────────────────────────────────────
Obecne saldo: 0 + 500 - 200 + 700 = 1,000 PLN
```

**Zalety:**
- ✅ Pełna historia (audit trail)
- ✅ Możliwość odtworzenia stanu z dowolnego momentu
- ✅ Analiza "co się stało i dlaczego?"

### 21.3 Event Sourcing vs Traditional Persistence

| Aspekt | Traditional (State-based) | Event Sourcing |
|--------|---------------------------|----------------|
| **Zapis** | UPDATE current state | APPEND new event |
| **Historia** | Utracona | Pełna |
| **Audit** | Trzeba dodać osobno | Built-in |
| **Debugging** | Trudne | Łatwe (replay) |
| **Storage** | Mniej | Więcej (wszystkie events) |
| **Queries** | Szybkie | Wymaga projekcji |
| **Delete** | DELETE row | Nie! (tylko nowy event) |

### 21.4 W EventMaster - Event Sourced Aggregate

**Tradycyjne podejście:**
```java
@Entity
@Table(name = "events")
public class Event {
    @Id
    private UUID id;
    private String name;
    private EventStatus status;  // DRAFT, PUBLISHED, CANCELLED
    
    // UPDATE changes status
    public void publish() {
        this.status = EventStatus.PUBLISHED;
        // Stara wartość DRAFT jest UTRACONA! 😱
    }
}
```

**Event Sourcing:**
```java
public class Event {  // Nie jest @Entity!
    
    private UUID id;
    private String name;
    private EventStatus status;
    private Long version = 0L;
    
    @Transient
    private List<DomainEvent> uncommittedEvents = new ArrayList<>();
    
    // Business methods emitują events
    public void publish() {
        // Validacja
        if (status == EventStatus.PUBLISHED) {
            throw new IllegalStateException("Already published");
        }
        
        // Emit event (nie zmieniamy stanu bezpośrednio!)
        EventPublishedEvent event = new EventPublishedEvent(
            this.id,
            Instant.now()
        );
        
        uncommittedEvents.add(event);
        
        // Zastosuj event do siebie
        apply(event);
    }
    
    // Apply event changes state
    private void apply(EventPublishedEvent event) {
        this.status = EventStatus.PUBLISHED;
        this.version++;
    }
    
    // Reconstruct from events
    public static Event from(List<DomainEvent> events) {
        Event event = new Event();
        events.forEach(event::apply);
        return event;
    }
    
    public List<DomainEvent> getUncommittedEvents() {
        return uncommittedEvents;
    }
    
    public void markEventsAsCommitted() {
        uncommittedEvents.clear();
    }
}
```

### 21.5 Event Store Schema

**PostgreSQL Implementation:**

```sql
CREATE TABLE event_store (
    id BIGSERIAL PRIMARY KEY,
    
    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,  -- 'Event', 'Booking', etc.
    
    -- Event identification
    sequence_number BIGINT NOT NULL,  -- Version within aggregate
    event_type VARCHAR(200) NOT NULL,  -- 'EventCreatedEvent', etc.
    event_id UUID NOT NULL UNIQUE,     -- Global unique event ID
    
    -- Event data
    payload JSONB NOT NULL,            -- Event data as JSON
    metadata JSONB,                    -- Causation ID, correlation ID, user ID
    
    -- Timestamps
    occurred_at TIMESTAMP NOT NULL,    -- Business time
    stored_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(aggregate_id, sequence_number)
);

-- Indexes for performance
CREATE INDEX idx_event_store_aggregate 
    ON event_store(aggregate_id, sequence_number);

CREATE INDEX idx_event_store_type 
    ON event_store(aggregate_type, stored_at);

CREATE INDEX idx_event_store_occurred 
    ON event_store(occurred_at);

CREATE INDEX idx_event_store_event_type 
    ON event_store(event_type);

-- Index for JSONB queries
CREATE INDEX idx_event_store_payload 
    ON event_store USING gin(payload);
```

### 21.6 Event Store Repository

```java
@Repository
@RequiredArgsConstructor
public class EventStoreRepository {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    
    /**
     * Append events to Event Store
     */
    @Transactional
    public void appendEvents(
        UUID aggregateId, 
        String aggregateType,
        Long expectedVersion,
        List<DomainEvent> events
    ) {
        // Optimistic locking check
        Long currentVersion = getCurrentVersion(aggregateId);
        if (!currentVersion.equals(expectedVersion)) {
            throw new OptimisticLockException(
                "Expected version " + expectedVersion + 
                " but was " + currentVersion
            );
        }
        
        // Append events
        String sql = """
            INSERT INTO event_store (
                aggregate_id, 
                aggregate_type, 
                sequence_number, 
                event_type, 
                event_id,
                payload, 
                metadata,
                occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)
            """;
        
        for (int i = 0; i < events.size(); i++) {
            DomainEvent event = events.get(i);
            long sequenceNumber = expectedVersion + i + 1;
            
            String payload = objectMapper.writeValueAsString(event);
            String metadata = createMetadata(event);
            
            jdbcTemplate.update(sql,
                aggregateId,
                aggregateType,
                sequenceNumber,
                event.getClass().getSimpleName(),
                event.getEventId(),
                payload,
                metadata,
                event.getOccurredAt()
            );
        }
    }
    
    /**
     * Load all events for aggregate
     */
    public List<DomainEvent> loadEvents(UUID aggregateId) {
        String sql = """
            SELECT event_type, payload
            FROM event_store
            WHERE aggregate_id = ?
            ORDER BY sequence_number ASC
            """;
        
        return jdbcTemplate.query(sql, 
            (rs, rowNum) -> {
                String eventType = rs.getString("event_type");
                String payload = rs.getString("payload");
                
                return deserializeEvent(eventType, payload);
            },
            aggregateId
        );
    }
    
    /**
     * Load events from specific version
     */
    public List<DomainEvent> loadEventsFromVersion(
        UUID aggregateId, 
        Long fromVersion
    ) {
        String sql = """
            SELECT event_type, payload
            FROM event_store
            WHERE aggregate_id = ?
              AND sequence_number > ?
            ORDER BY sequence_number ASC
            """;
        
        return jdbcTemplate.query(sql,
            (rs, rowNum) -> deserializeEvent(
                rs.getString("event_type"),
                rs.getString("payload")
            ),
            aggregateId,
            fromVersion
        );
    }
    
    /**
     * Get current version of aggregate
     */
    private Long getCurrentVersion(UUID aggregateId) {
        String sql = """
            SELECT COALESCE(MAX(sequence_number), 0)
            FROM event_store
            WHERE aggregate_id = ?
            """;
        
        return jdbcTemplate.queryForObject(sql, Long.class, aggregateId);
    }
    
    private DomainEvent deserializeEvent(String eventType, String payload) {
        try {
            Class<?> eventClass = Class.forName(
                "com.eventmaster.events." + eventType
            );
            return (DomainEvent) objectMapper.readValue(payload, eventClass);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize event", e);
        }
    }
    
    private String createMetadata(DomainEvent event) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("causationId", event.getCausationId());
        metadata.put("correlationId", event.getCorrelationId());
        metadata.put("userId", event.getUserId());
        
        try {
            return objectMapper.writeValueAsString(metadata);
        } catch (Exception e) {
            return "{}";
        }
    }
}
```

### 21.7 Event Sourced Repository

```java
@Repository
@RequiredArgsConstructor
public class EventSourcedEventRepository {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Save aggregate by appending its uncommitted events
     */
    @Transactional
    public void save(Event event) {
        List<DomainEvent> uncommittedEvents = event.getUncommittedEvents();
        
        if (uncommittedEvents.isEmpty()) {
            return;  // Nothing to save
        }
        
        // Append to Event Store
        eventStore.appendEvents(
            event.getId(),
            "Event",
            event.getVersion() - uncommittedEvents.size(),  // Expected version
            uncommittedEvents
        );
        
        // Mark as committed
        event.markEventsAsCommitted();
    }
    
    /**
     * Load aggregate by replaying all its events
     */
    public Optional<Event> findById(UUID eventId) {
        List<DomainEvent> events = eventStore.loadEvents(eventId);
        
        if (events.isEmpty()) {
            return Optional.empty();
        }
        
        // Reconstruct from events
        Event event = Event.from(events);
        return Optional.of(event);
    }
}
```

### 21.8 Command Handler z Event Sourcing

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class PublishEventCommandHandler {
    
    private final EventSourcedEventRepository repository;
    
    @Transactional
    public void handle(PublishEventCommand command) {
        log.info("Publishing event: {}", command.getEventId());
        
        // 1. Load aggregate from Event Store
        Event event = repository.findById(command.getEventId())
            .orElseThrow(() -> new EntityNotFoundException(
                "Event not found: " + command.getEventId()
            ));
        
        // 2. Execute business logic (emits EventPublishedEvent)
        event.publish();
        
        // 3. Save (appends EventPublishedEvent to Event Store)
        repository.save(event);
        
        log.info("Event published: {}", command.getEventId());
    }
}
```

### 21.9 Testowanie Event Sourcing

```java
@SpringBootTest
@Testcontainers
class EventSourcingTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18");
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
    }
    
    @Autowired
    private EventStoreRepository eventStore;
    
    @Autowired
    private EventSourcedEventRepository eventRepository;
    
    @Test
    void shouldPersistEventsToEventStore() {
        // Given - create aggregate
        Event event = Event.create(
            UUID.randomUUID(),
            "JavaConf 2025",
            "Java conference",
            "Warsaw, Poland",
            LocalDateTime.of(2025, 6, 15, 9, 0)
        );
        
        // When - save (appends EventCreatedEvent)
        eventRepository.save(event);
        
        // Then - Event Store contains 1 event
        List<DomainEvent> events = eventStore.loadEvents(event.getId());
        assertThat(events).hasSize(1);
        assertThat(events.get(0)).isInstanceOf(EventCreatedEvent.class);
    }
    
    @Test
    void shouldReconstructAggregateFromEvents() {
        // Given - event with multiple state changes
        UUID eventId = UUID.randomUUID();
        Event event = Event.create(
            eventId,
            "KotlinConf 2025",
            "Kotlin conference",
            "Berlin, Germany",
            LocalDateTime.of(2025, 5, 10, 9, 0)
        );
        eventRepository.save(event);
        
        event.publish();  // Emits EventPublishedEvent
        eventRepository.save(event);
        
        event.cancel();   // Emits EventCancelledEvent
        eventRepository.save(event);
        
        // When - load from Event Store
        Event reconstructed = eventRepository.findById(eventId).orElseThrow();
        
        // Then - state reconstructed correctly
        assertThat(reconstructed.getStatus()).isEqualTo(EventStatus.CANCELLED);
        assertThat(reconstructed.getVersion()).isEqualTo(3L);
        
        // Event Store has 3 events
        List<DomainEvent> events = eventStore.loadEvents(eventId);
        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(EventCreatedEvent.class);
        assertThat(events.get(1)).isInstanceOf(EventPublishedEvent.class);
        assertThat(events.get(2)).isInstanceOf(EventCancelledEvent.class);
    }
    
    @Test
    void shouldEnforceOptimisticLocking() {
        // Given - event in Event Store
        UUID eventId = UUID.randomUUID();
        Event event = Event.create(eventId, "Test", "Desc", "Loc", LocalDateTime.now());
        eventRepository.save(event);
        
        // When - two concurrent modifications
        Event event1 = eventRepository.findById(eventId).orElseThrow();
        Event event2 = eventRepository.findById(eventId).orElseThrow();
        
        event1.publish();
        eventRepository.save(event1);  // OK, version 1 → 2
        
        event2.cancel();
        assertThatThrownBy(() -> eventRepository.save(event2))
            .isInstanceOf(OptimisticLockException.class);
    }
    
    @Test
    void shouldNeverDeleteEvents() {
        // Given - event created and cancelled
        UUID eventId = UUID.randomUUID();
        Event event = Event.create(eventId, "Test", "Desc", "Loc", LocalDateTime.now());
        eventRepository.save(event);
        
        event.cancel();
        eventRepository.save(event);
        
        // When - "delete" (just cancel event, don't delete from Event Store)
        // event.delete();  // NO DELETE METHOD!
        
        // Then - Event Store still has all events
        List<DomainEvent> events = eventStore.loadEvents(eventId);
        assertThat(events).hasSize(2);  // EventCreated + EventCancelled
    }
}
```

### 21.10 Zalety Event Sourcing

**1. Complete Audit Trail**
```java
// Możesz zobaczyć WSZYSTKIE zmiany
List<DomainEvent> history = eventStore.loadEvents(eventId);
history.forEach(event -> 
    log.info("Event: {} at {}", event.getClass(), event.getOccurredAt())
);
```

**2. Temporal Queries** (Zobacz Temat 25)
```java
// "Jak wyglądał event 3 miesiące temu?"
Event pastState = eventRepository.findByIdAtTime(
    eventId, 
    Instant.now().minus(90, ChronoUnit.DAYS)
);
```

**3. Event Replay** (Debugging)
```java
// Odtwórz scenariusz który spowodował bug
List<DomainEvent> events = eventStore.loadEvents(problematicEventId);
Event event = Event.from(events);
// Debug step by step
```

**4. Projections** (Multiple Read Models)
```java
// Możesz stworzyć NOWE Read Models z istniejących events!
events.forEach(event -> newProjector.handle(event));
```

### 21.11 Wady Event Sourcing

**1. Complexity**
- Więcej kodu
- Trudniejsze do zrozumienia
- Wymaga discipline (immutability)

**2. Storage**
- Event Store rośnie w nieskończoność
- Wymaga Event Compaction (Temat 28)

**3. Eventual Consistency**
- Read Models są opóźnione
- Użytkownicy muszą to zaakceptować

**4. Event Versioning**
- Zmiany w strukturze events są trudne
- Wymaga Upcasting (Temat 24)

**5. GDPR**
- "Right to be forgotten" vs immutable events
- Wymaga Crypto Shredding (Temat 27)

### 21.12 Kiedy Używać Event Sourcing?

**✅ UŻYJ gdy:**
- Audit trail jest kluczowy (finanse, medycyna)
- Potrzebujesz temporal queries
- Analityka historyczna jest ważna
- Debugging złożonych scenariuszy
- Domain-driven design z bogatą logiką

**❌ NIE UŻYWAJ gdy:**
- Prosty CRUD
- Brak wymagań audytowych
- Zespół nie ma doświadczenia
- Performance > completeness

### 21.13 Hybrid Approach (Temat 30)

Możesz kombinować:
```
Event Sourcing dla KRYTYCZNYCH aggregates (Booking, Payment)
+
Traditional persistence dla PROSTYCH entities (User, Category)
```

### 21.14 Podsumowanie Tematu 21

**Kluczowe Pojęcia:**
- Event Sourcing = persist changes as events
- Event Store = append-only log
- Reconstruction = replay events
- No DELETE = tylko nowe events

**Implementacja:**
- PostgreSQL Event Store (JSONB)
- Event Sourced Repository
- Optimistic Locking przez version
- Testcontainers dla testów

**Następny temat:** [Event Store Implementation](#temat-22-event-store-implementation)

---

## Temat 22: Event Store - Implementation

### 22.1 Wymagania dla Event Store

Event Store musi być:
1. **Append-only** - tylko dodawanie, nigdy update/delete
2. **Ordered** - zachowana kolejność events
3. **Immutable** - events nigdy się nie zmieniają
4. **Consistent** - optimistic locking
5. **Fast for writes** - append jest szybki
6. **Fast for reads** - load events by aggregate ID

### 22.2 PostgreSQL vs Dedicated Event Store

| Feature | PostgreSQL | EventStoreDB | Apache Kafka |
|---------|-----------|--------------|--------------|
| **Append-only** | ✅ (constraint) | ✅ Built-in | ✅ Built-in |
| **Queries** | ✅ SQL power | ✅ Good | ❌ Limited |
| **Projections** | ➖ Manual | ✅ Built-in | ✅ Kafka Streams |
| **Snapshots** | ➖ Manual | ✅ Built-in | ➖ Manual |
| **Learning curve** | ✅ Low | ➖ Medium | ➖ Medium |
| **Ops complexity** | ✅ Low | ➖ Medium | ➖ High |
| **Cost** | ✅ Low | ➖ License | ✅ Open-source |

**Wybór dla EventMaster: PostgreSQL 18**
- Już używamy PostgreSQL
- Wystarczająca wydajność dla 90% przypadków
- Łatwiejsze operacyjnie
- Potężne zapytania SQL

### 22.3 Advanced Event Store Schema

```sql
CREATE TABLE event_store (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    
    -- Event identification
    sequence_number BIGINT NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_version INTEGER NOT NULL DEFAULT 1,  -- Event schema version
    
    -- Event data
    payload JSONB NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Timestamps
    occurred_at TIMESTAMP NOT NULL,
    stored_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Causality
    causation_id UUID,      -- Event that caused this event
    correlation_id UUID,    -- Original command/request ID
    
    -- User tracking
    user_id UUID,
    user_email VARCHAR(255),
    
    -- Checksum for integrity
    checksum VARCHAR(64),   -- SHA-256 of payload
    
    -- Constraints
    UNIQUE(aggregate_id, sequence_number),
    CHECK (sequence_number > 0)
);

-- Indexes
CREATE INDEX idx_event_store_aggregate 
    ON event_store(aggregate_id, sequence_number);

CREATE INDEX idx_event_store_type 
    ON event_store(aggregate_type, stored_at DESC);

CREATE INDEX idx_event_store_occurred 
    ON event_store(occurred_at);

CREATE INDEX idx_event_store_correlation 
    ON event_store(correlation_id) 
    WHERE correlation_id IS NOT NULL;

CREATE INDEX idx_event_store_user 
    ON event_store(user_id) 
    WHERE user_id IS NOT NULL;

-- GIN index for JSONB queries
CREATE INDEX idx_event_store_payload_gin 
    ON event_store USING gin(payload);

CREATE INDEX idx_event_store_metadata_gin 
    ON event_store USING gin(metadata);
```

### 22.4 Event Store Entity

```java
@Entity
@Table(name = "event_store")
@Data
@NoArgsConstructor
public class StoredEvent {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;
    
    @Column(name = "aggregate_type", nullable = false, length = 100)
    private String aggregateType;
    
    @Column(name = "sequence_number", nullable = false)
    private Long sequenceNumber;
    
    @Column(name = "event_type", nullable = false, length = 200)
    private String eventType;
    
    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;
    
    @Column(name = "event_version", nullable = false)
    private Integer eventVersion = 1;
    
    @Type(JsonBinaryType.class)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;
    
    @Type(JsonBinaryType.class)
    @Column(name = "metadata", columnDefinition = "jsonb")
    private String metadata;
    
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
    
    @Column(name = "stored_at", nullable = false)
    private Instant storedAt;
    
    @Column(name = "causation_id")
    private UUID causationId;
    
    @Column(name = "correlation_id")
    private UUID correlationId;
    
    @Column(name = "user_id")
    private UUID userId;
    
    @Column(name = "user_email")
    private String userEmail;
    
    @Column(name = "checksum", length = 64)
    private String checksum;
    
    @PrePersist
    protected void onCreate() {
        if (storedAt == null) {
            storedAt = Instant.now();
        }
        if (eventId == null) {
            eventId = UUID.randomUUID();
        }
        if (checksum == null) {
            checksum = calculateChecksum();
        }
    }
    
    private String calculateChecksum() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
    
    public boolean verifyChecksum() {
        return checksum.equals(calculateChecksum());
    }
}
```

### 22.5 Advanced Event Store Repository

```java
@Repository
@RequiredArgsConstructor
@Slf4j
public class AdvancedEventStoreRepository {
    
    private final EntityManager entityManager;
    private final ObjectMapper objectMapper;
    
    /**
     * Append events with full metadata
     */
    @Transactional
    public void appendEvents(
        UUID aggregateId,
        String aggregateType,
        Long expectedVersion,
        List<DomainEvent> events,
        EventMetadata metadata
    ) {
        // Optimistic locking
        Long currentVersion = getCurrentVersion(aggregateId);
        if (!currentVersion.equals(expectedVersion)) {
            throw new ConcurrencyException(
                String.format(
                    "Aggregate %s: expected version %d but was %d",
                    aggregateId, expectedVersion, currentVersion
                )
            );
        }
        
        // Append events
        for (int i = 0; i < events.size(); i++) {
            DomainEvent event = events.get(i);
            long sequenceNumber = expectedVersion + i + 1;
            
            StoredEvent storedEvent = new StoredEvent();
            storedEvent.setAggregateId(aggregateId);
            storedEvent.setAggregateType(aggregateType);
            storedEvent.setSequenceNumber(sequenceNumber);
            storedEvent.setEventType(event.getClass().getSimpleName());
            storedEvent.setEventId(event.getEventId());
            storedEvent.setEventVersion(event.getVersion());
            storedEvent.setPayload(serializeEvent(event));
            storedEvent.setMetadata(serializeMetadata(metadata));
            storedEvent.setOccurredAt(event.getOccurredAt());
            storedEvent.setCausationId(metadata.getCausationId());
            storedEvent.setCorrelationId(metadata.getCorrelationId());
            storedEvent.setUserId(metadata.getUserId());
            storedEvent.setUserEmail(metadata.getUserEmail());
            
            entityManager.persist(storedEvent);
            
            log.debug("Event appended: {} seq={} for aggregate={}", 
                event.getClass().getSimpleName(), sequenceNumber, aggregateId);
        }
    }
    
    /**
     * Load events with optional version range
     */
    public List<DomainEvent> loadEvents(
        UUID aggregateId,
        Long fromVersion,
        Long toVersion
    ) {
        String jpql = """
            SELECT e FROM StoredEvent e
            WHERE e.aggregateId = :aggregateId
            AND e.sequenceNumber > :fromVersion
            AND e.sequenceNumber <= :toVersion
            ORDER BY e.sequenceNumber ASC
            """;
        
        return entityManager.createQuery(jpql, StoredEvent.class)
            .setParameter("aggregateId", aggregateId)
            .setParameter("fromVersion", fromVersion != null ? fromVersion : 0L)
            .setParameter("toVersion", toVersion != null ? toVersion : Long.MAX_VALUE)
            .getResultStream()
            .map(this::deserializeEvent)
            .collect(Collectors.toList());
    }
    
    /**
     * Load events by type (for projections)
     */
    public Stream<DomainEvent> loadEventsByType(
        String eventType,
        Instant from,
        Instant to
    ) {
        String jpql = """
            SELECT e FROM StoredEvent e
            WHERE e.eventType = :eventType
            AND e.occurredAt >= :from
            AND e.occurredAt < :to
            ORDER BY e.occurredAt ASC
            """;
        
        return entityManager.createQuery(jpql, StoredEvent.class)
            .setParameter("eventType", eventType)
            .setParameter("from", from)
            .setParameter("to", to)
            .getResultStream()
            .map(this::deserializeEvent);
    }
    
    /**
     * Load all events (for rebuilding projections)
     */
    public Stream<DomainEvent> loadAllEvents(Long batchSize) {
        String jpql = """
            SELECT e FROM StoredEvent e
            ORDER BY e.storedAt ASC
            """;
        
        return entityManager.createQuery(jpql, StoredEvent.class)
            .setMaxResults(batchSize.intValue())
            .getResultStream()
            .map(this::deserializeEvent);
    }
    
    /**
     * Get aggregate version
     */
    public Long getCurrentVersion(UUID aggregateId) {
        String jpql = """
            SELECT COALESCE(MAX(e.sequenceNumber), 0)
            FROM StoredEvent e
            WHERE e.aggregateId = :aggregateId
            """;
        
        return entityManager.createQuery(jpql, Long.class)
            .setParameter("aggregateId", aggregateId)
            .getSingleResult();
    }
    
    /**
     * Check if aggregate exists
     */
    public boolean exists(UUID aggregateId) {
        String jpql = """
            SELECT COUNT(e) > 0
            FROM StoredEvent e
            WHERE e.aggregateId = :aggregateId
            """;
        
        return entityManager.createQuery(jpql, Boolean.class)
            .setParameter("aggregateId", aggregateId)
            .getSingleResult();
    }
    
    /**
     * Get event count for aggregate
     */
    public Long getEventCount(UUID aggregateId) {
        String jpql = """
            SELECT COUNT(e)
            FROM StoredEvent e
            WHERE e.aggregateId = :aggregateId
            """;
        
        return entityManager.createQuery(jpql, Long.class)
            .setParameter("aggregateId", aggregateId)
            .getSingleResult();
    }
    
    /**
     * Verify Event Store integrity
     */
    public List<UUID> verifyIntegrity() {
        String jpql = "SELECT e FROM StoredEvent e";
        
        return entityManager.createQuery(jpql, StoredEvent.class)
            .getResultStream()
            .filter(e -> !e.verifyChecksum())
            .map(StoredEvent::getEventId)
            .collect(Collectors.toList());
    }
    
    private String serializeEvent(DomainEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize event", e);
        }
    }
    
    private String serializeMetadata(EventMetadata metadata) {
        try {
            return objectMapper.writeValueAsString(metadata);
        } catch (Exception e) {
            return "{}";
        }
    }
    
    private DomainEvent deserializeEvent(StoredEvent storedEvent) {
        try {
            Class<?> eventClass = Class.forName(
                "com.eventmaster.events." + storedEvent.getEventType()
            );
            return (DomainEvent) objectMapper.readValue(
                storedEvent.getPayload(), 
                eventClass
            );
        } catch (Exception e) {
            throw new DeserializationException(
                "Failed to deserialize event: " + storedEvent.getEventType(), 
                e
            );
        }
    }
}
```

### 22.6 Event Metadata

```java
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EventMetadata {
    private UUID causationId;      // Which event caused this?
    private UUID correlationId;    // Original request/command ID
    private UUID userId;           // Who triggered this?
    private String userEmail;
    private String ipAddress;
    private String userAgent;
    private Map<String, String> customData;
    
    public static EventMetadata fromContext() {
        // Extract from SecurityContext, MDC, etc.
        return EventMetadata.builder()
            .userId(SecurityContextHolder.getUserId())
            .userEmail(SecurityContextHolder.getUserEmail())
            .correlationId(MDC.get("correlationId"))
            .ipAddress(RequestContextHolder.getIpAddress())
            .userAgent(RequestContextHolder.getUserAgent())
            .build();
    }
}
```

### 22.7 Testowanie Event Store

```java
@SpringBootTest
@Testcontainers
class EventStoreIntegrityTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18");
    
    @Autowired
    private AdvancedEventStoreRepository eventStore;
    
    @Test
    void shouldPreventDuplicateSequenceNumbers() {
        // Given
        UUID aggregateId = UUID.randomUUID();
        DomainEvent event1 = new EventCreatedEvent(/* ... */);
        
        // When - append event twice with same sequence number
        eventStore.appendEvents(aggregateId, "Event", 0L, List.of(event1), metadata());
        
        // Then - second append should fail
        assertThatThrownBy(() -> 
            eventStore.appendEvents(aggregateId, "Event", 0L, List.of(event1), metadata())
        ).isInstanceOf(DataIntegrityViolationException.class);
    }
    
    @Test
    void shouldVerifyChecksums() {
        // Given - event in Event Store
        UUID aggregateId = UUID.randomUUID();
        DomainEvent event = new EventCreatedEvent(/* ... */);
        eventStore.appendEvents(aggregateId, "Event", 0L, List.of(event), metadata());
        
        // When - verify integrity
        List<UUID> corruptedEvents = eventStore.verifyIntegrity();
        
        // Then - no corrupted events
        assertThat(corruptedEvents).isEmpty();
    }
    
    @Test
    void shouldLoadEventsInCorrectOrder() {
        // Given - 10 events
        UUID aggregateId = UUID.randomUUID();
        List<DomainEvent> events = IntStream.range(0, 10)
            .mapToObj(i -> new TestEvent(i))
            .collect(Collectors.toList());
        
        eventStore.appendEvents(aggregateId, "Test", 0L, events, metadata());
        
        // When - load events
        List<DomainEvent> loaded = eventStore.loadEvents(aggregateId, 0L, 10L);
        
        // Then - order preserved
        assertThat(loaded).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(((TestEvent) loaded.get(i)).getSequence()).isEqualTo(i);
        }
    }
    
    @Test
    void shouldLoadEventsByVersionRange() {
        // Given - 10 events (versions 1-10)
        UUID aggregateId = UUID.randomUUID();
        List<DomainEvent> events = createTestEvents(10);
        eventStore.appendEvents(aggregateId, "Test", 0L, events, metadata());
        
        // When - load events 5-8
        List<DomainEvent> loaded = eventStore.loadEvents(aggregateId, 4L, 8L);
        
        // Then - only 4 events (5, 6, 7, 8)
        assertThat(loaded).hasSize(4);
    }
}
```

### 22.8 Podsumowanie Tematu 22

**Kluczowe Pojęcia:**
- Event Store = append-only database
- Checksum = data integrity
- Metadata = causation, correlation, user tracking
- Optimistic locking = version check

**Best Practices:**
- ✅ Używaj JSONB dla payload
- ✅ Dodaj checksum dla integrity
- ✅ Track causation/correlation
- ✅ Index strategically
- ✅ Verify integrity periodically

**Następny temat:** [Event Versioning](#temat-23-event-versioning)

---

## Temat 23: Event Versioning

### 23.1 Problem: Ewolucja Events

**Scenariusz:** Mamy EventCreatedEvent w produkcji:
```java
// Version 1 (deployed 6 months ago)
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String location  // "Warsaw, Poland"
) {}
```

**Nowe wymaganie:** Rozdziel location na city i country:
```java
// Version 2 (new)
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String city,     // "Warsaw"
    String country   // "Poland"
) {}
```

**Problem:**
- Event Store ma 10,000 events w wersji 1
- Nie możemy ich usunąć (immutable!)
- Deserializacja nie zadziała

### 23.2 Strategie Versioning

#### Strategia 1: Additive Changes Only (Zalecana)

```java
// Version 1
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String location
) {}

// Version 2 - TYLKO dodajemy pola
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String location,     // Zachowane dla backward compatibility
    String city,         // Nowe
    String country       // Nowe
) {
    // Constructor dla v1 (upcasting)
    public EventCreatedEvent(UUID eventId, String name, String location) {
        this(eventId, name, location, 
             extractCity(location), 
             extractCountry(location));
    }
}
```

#### Strategia 2: New Event Type

```java
// Keep old event
public record EventCreatedEvent { /* v1 */ }

// Create new event
public record EventCreatedEventV2 {
    UUID eventId;
    String name;
    String city;
    String country;
}

// Handler obsługuje obie wersje
@EventHandler
public void handle(EventCreatedEvent event) { /* old */ }

@EventHandler
public void handle(EventCreatedEventV2 event) { /* new */ }
```

#### Strategia 3: Event Upcasting (Zobacz Temat 24)

```java
// Transform v1 → v2 podczas deserializacji
public class EventUpcaster {
    public EventCreatedEventV2 upcast(EventCreatedEventV1 v1) {
        return new EventCreatedEventV2(
            v1.eventId(),
            v1.name(),
            extractCity(v1.location()),
            extractCountry(v1.location())
        );
    }
}
```

### 23.3 Implementacja - Event Version Field

```java
public interface DomainEvent {
    UUID getEventId();
    Instant getOccurredAt();
    int getVersion();  // Event schema version
}

// Version 1
public record EventCreatedEvent(
    UUID eventId,
    String name,
    String location,
    Instant occurredAt
) implements DomainEvent {
    @Override
    public int getVersion() {
        return 1;
    }
}

// Version 2
public record EventCreatedEventV2(
    UUID eventId,
    String name,
    String city,
    String country,
    Instant occurredAt
) implements DomainEvent {
    @Override
    public int getVersion() {
        return 2;
    }
}
```

### 23.4 Deserializacja z Versioning

```java
@Component
@RequiredArgsConstructor
public class VersionedEventDeserializer {
    
    private final ObjectMapper objectMapper;
    private final Map<String, EventUpcaster> upcasters = new ConcurrentHashMap<>();
    
    public DomainEvent deserialize(String eventType, int version, String payload) {
        // Find upcaster chain
        List<EventUpcaster> chain = getUpcasterChain(eventType, version);
        
        // Deserialize to original version
        DomainEvent event = deserializeToVersion(eventType, version, payload);
        
        // Upcast to latest version
        for (EventUpcaster upcaster : chain) {
            event = upcaster.upcast(event);
        }
        
        return event;
    }
    
    private DomainEvent deserializeToVersion(
        String eventType, 
        int version, 
        String payload
    ) {
        String className = String.format(
            "com.eventmaster.events.%s%s",
            eventType,
            version > 1 ? "V" + version : ""
        );
        
        try {
            Class<?> eventClass = Class.forName(className);
            return (DomainEvent) objectMapper.readValue(payload, eventClass);
        } catch (Exception e) {
            throw new DeserializationException(
                "Failed to deserialize " + className, e
            );
        }
    }
    
    private List<EventUpcaster> getUpcasterChain(String eventType, int fromVersion) {
        // Return chain of upcasters: v1→v2→v3→...→latest
        return upcasters.values().stream()
            .filter(u -> u.supports(eventType))
            .filter(u -> u.getFromVersion() >= fromVersion)
            .sorted(Comparator.comparing(EventUpcaster::getFromVersion))
            .collect(Collectors.toList());
    }
}
```

### 23.5 Testowanie Event Versioning

```java
@SpringBootTest
class EventVersioningTest {
    
    @Autowired
    private VersionedEventDeserializer deserializer;
    
    @Test
    void shouldDeserializeV1Events() {
        // Given - V1 event in Event Store
        String v1Payload = """
            {
                "eventId": "123e4567-e89b-12d3-a456-426614174000",
                "name": "Test Event",
                "location": "Warsaw, Poland",
                "occurredAt": "2024-01-01T10:00:00Z"
            }
            """;
        
        // When - deserialize
        DomainEvent event = deserializer.deserialize("EventCreatedEvent", 1, v1Payload);
        
        // Then - deserialized to V1
        assertThat(event).isInstanceOf(EventCreatedEvent.class);
        assertThat(event.getVersion()).isEqualTo(1);
    }
    
    @Test
    void shouldUpcastV1ToV2() {
        // Given - V1 event
        String v1Payload = """
            {
                "eventId": "123e4567-e89b-12d3-a456-426614174000",
                "name": "Test Event",
                "location": "Berlin, Germany"
            }
            """;
        
        // When - deserialize with upcasting
        DomainEvent event = deserializer.deserialize("EventCreatedEvent", 1, v1Payload);
        
        // Then - upcasted to V2
        assertThat(event).isInstanceOf(EventCreatedEventV2.class);
        
        EventCreatedEventV2 v2 = (EventCreatedEventV2) event;
        assertThat(v2.city()).isEqualTo("Berlin");
        assertThat(v2.country()).isEqualTo("Germany");
    }
}
```

### 23.6 Podsumowanie Tematu 23

**Strategie Versioning:**
1. Additive Changes Only (best)
2. New Event Type
3. Event Upcasting

**Best Practices:**
- ✅ Dodaj version field do wszystkich events
- ✅ NIGDY nie usuwaj pól (tylko deprecate)
- ✅ Preferuj additive changes
- ✅ Dokumentuj zmiany w CHANGELOG

**Następny temat:** [Upcasting Events](#temat-24-upcasting-events)

---

## Temat 24: Upcasting Events

### 24.1 Definicja

**Upcasting** = transformacja starych wersji events do nowych wersji podczas deserializacji.

### 24.2 Event Upcaster Interface

```java
public interface EventUpcaster<FROM extends DomainEvent, TO extends DomainEvent> {
    
    /**
     * Which event type this upcaster supports
     */
    String getEventType();
    
    /**
     * From which version
     */
    int getFromVersion();
    
    /**
     * To which version
     */
    int getToVersion();
    
    /**
     * Transform old event to new event
     */
    TO upcast(FROM oldEvent);
    
    /**
     * Check if this upcaster supports given event type
     */
    default boolean supports(String eventType) {
        return getEventType().equals(eventType);
    }
}
```

### 24.3 Implementacja Upcastera

```java
@Component
public class EventCreatedEventV1ToV2Upcaster 
    implements EventUpcaster<EventCreatedEvent, EventCreatedEventV2> {
    
    @Override
    public String getEventType() {
        return "EventCreatedEvent";
    }
    
    @Override
    public int getFromVersion() {
        return 1;
    }
    
    @Override
    public int getToVersion() {
        return 2;
    }
    
    @Override
    public EventCreatedEventV2 upcast(EventCreatedEvent v1) {
        // Parse location "City, Country"
        String[] parts = v1.location().split(",\\s*");
        String city = parts.length > 0 ? parts[0] : "";
        String country = parts.length > 1 ? parts[1] : "";
        
        return new EventCreatedEventV2(
            v1.eventId(),
            v1.name(),
            city,
            country,
            v1.occurredAt()
        );
    }
}
```

### 24.4 Chain Upcasting (V1 → V2 → V3)

```java
// V1 → V2
@Component
public class EventV1ToV2Upcaster implements EventUpcaster<EventV1, EventV2> {
    @Override
    public EventV2 upcast(EventV1 v1) {
        return new EventV2(
            v1.eventId(),
            v1.name(),
            extractCity(v1.location()),
            extractCountry(v1.location())
        );
    }
}

// V2 → V3
@Component
public class EventV2ToV3Upcaster implements EventUpcaster<EventV2, EventV3> {
    @Override
    public EventV3 upcast(EventV2 v2) {
        return new EventV3(
            v2.eventId(),
            v2.name(),
            new Location(v2.city(), v2.country()),  // Wrapped in value object
            Instant.now()  // Added timestamp
        );
    }
}

// Upcaster chain automatically applies: V1 → V2 → V3
```

### 24.5 Lazy vs Eager Upcasting

**Lazy (On-Read):**
```java
// Upcast podczas deserializacji
public DomainEvent loadEvent(UUID eventId) {
    StoredEvent stored = repository.find(eventId);
    return upcast(stored);  // Upcast here
}
```

**Eager (Background Job):**
```java
@Scheduled(cron = "0 2 * * * *")  // 2 AM daily
public void upcastOldEvents() {
    // Find all V1 events
    List<StoredEvent> v1Events = eventStore.findByVersion(1);
    
    for (StoredEvent stored : v1Events) {
        // Deserialize V1
        EventV1 v1 = deserialize(stored);
        
        // Upcast to V2
        EventV2 v2 = upcaster.upcast(v1);
        
        // Update Event Store
        stored.setPayload(serialize(v2));
        stored.setEventVersion(2);
        repository.save(stored);
    }
}
```

**Trade-offs:**
- Lazy: Simple, no migration needed, slower reads
- Eager: Faster reads, requires migration, complex

### 24.6 Testowanie Upcasting

```java
@SpringBootTest
class EventUpcastingTest {
    
    @Autowired
    private EventCreatedEventV1ToV2Upcaster upcaster;
    
    @Test
    void shouldUpcastV1ToV2() {
        // Given - V1 event
        EventCreatedEvent v1 = new EventCreatedEvent(
            UUID.randomUUID(),
            "JavaConf 2025",
            "Warsaw, Poland",
            Instant.now()
        );
        
        // When - upcast
        EventCreatedEventV2 v2 = upcaster.upcast(v1);
        
        // Then - fields extracted
        assertThat(v2.city()).isEqualTo("Warsaw");
        assertThat(v2.country()).isEqualTo("Poland");
        assertThat(v2.eventId()).isEqualTo(v1.eventId());
    }
    
    @Test
    void shouldHandleInvalidLocationFormat() {
        // Given - V1 with invalid location
        EventCreatedEvent v1 = new EventCreatedEvent(
            UUID.randomUUID(),
            "Test",
            "InvalidLocation",  // No comma!
            Instant.now()
        );
        
        // When - upcast
        EventCreatedEventV2 v2 = upcaster.upcast(v1);
        
        // Then - graceful handling
        assertThat(v2.city()).isEqualTo("InvalidLocation");
        assertThat(v2.country()).isEqualTo("");
    }
}
```

### 24.7 Podsumowanie Tematu 24

**Kluczowe Pojęcia:**
- Upcasting = transformacja podczas deserializacji
- Chain upcasting = V1 → V2 → V3 → ...
- Lazy vs Eager upcasting

**Best Practices:**
- ✅ Preferuj lazy upcasting (prostsze)
- ✅ Testuj wszystkie upcasters
- ✅ Dokumentuj transformacje
- ✅ Handle edge cases gracefully

**Następny temat:** [Temporal Queries](#temat-25-temporal-queries)

---

## Temat 25: Temporal Queries

### 25.1 Definicja

**Temporal Query** = zapytanie o stan aggregate w konkretnym momencie przeszłości.

### 25.2 Przykłady Use Cases

1. **Audyt finansowy:** "Jakie było saldo konta 31 grudnia 2024?"
2. **Debugging:** "Co było stanem eventu gdy błąd wystąpił?"
3. **Raportowanie:** "Ile było aktywnych eventów w Q3 2024?"
4. **Compliance:** "Kto miał dostęp do danych 6 miesięcy temu?"

### 25.3 Implementacja - Point-in-Time Queries

```java
@Repository
@RequiredArgsConstructor
public class TemporalEventRepository {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Get aggregate state at specific point in time
     */
    public Optional<Event> findByIdAtTime(UUID eventId, Instant pointInTime) {
        // Load events that occurred before pointInTime
        List<DomainEvent> events = eventStore.loadEventsBeforeTime(
            eventId, 
            pointInTime
        );
        
        if (events.isEmpty()) {
            return Optional.empty();
        }
        
        // Reconstruct state
        Event event = Event.from(events);
        return Optional.of(event);
    }
    
    /**
     * Get aggregate state at specific version
     */
    public Optional<Event> findByIdAtVersion(UUID eventId, Long version) {
        List<DomainEvent> events = eventStore.loadEvents(
            eventId,
            0L,
            version
        );
        
        if (events.isEmpty()) {
            return Optional.empty();
        }
        
        Event event = Event.from(events);
        return Optional.of(event);
    }
}

// Event Store Repository extension
@Repository
public class EventStoreRepository {
    
    public List<DomainEvent> loadEventsBeforeTime(
        UUID aggregateId, 
        Instant pointInTime
    ) {
        String jpql = """
            SELECT e FROM StoredEvent e
            WHERE e.aggregateId = :aggregateId
            AND e.occurredAt < :pointInTime
            ORDER BY e.sequenceNumber ASC
            """;
        
        return entityManager.createQuery(jpql, StoredEvent.class)
            .setParameter("aggregateId", aggregateId)
            .setParameter("pointInTime", pointInTime)
            .getResultStream()
            .map(this::deserializeEvent)
            .collect(Collectors.toList());
    }
}
```

### 25.4 Temporal Queries dla Read Models

```java
@Service
@RequiredArgsConstructor
public class TemporalProjectionService {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Rebuild projection for specific time period
     */
    public EventStatistics getStatisticsAtTime(Instant pointInTime) {
        EventStatistics stats = new EventStatistics();
        
        // Load all events before pointInTime
        Stream<DomainEvent> events = eventStore.loadAllEventsBeforeTime(pointInTime);
        
        // Project to statistics
        events.forEach(event -> {
            if (event instanceof EventCreatedEvent) {
                stats.incrementTotalEvents();
            } else if (event instanceof EventPublishedEvent) {
                stats.incrementPublishedEvents();
            }
        });
        
        return stats;
    }
    
    /**
     * Compare states at two points in time
     */
    public StateDiff compareStates(UUID eventId, Instant time1, Instant time2) {
        Event state1 = temporalRepo.findByIdAtTime(eventId, time1).orElse(null);
        Event state2 = temporalRepo.findByIdAtTime(eventId, time2).orElse(null);
        
        return StateDiff.between(state1, state2);
    }
}
```

### 25.5 Temporal Queries w SQL (PostgreSQL)

```sql
-- Get all events for aggregate at specific time
SELECT 
    e.aggregate_id,
    e.sequence_number,
    e.event_type,
    e.payload
FROM event_store e
WHERE e.aggregate_id = '123e4567-e89b-12d3-a456-426614174000'
  AND e.occurred_at < '2024-12-31 23:59:59'
ORDER BY e.sequence_number;

-- Count events by type in time period
SELECT 
    e.event_type,
    COUNT(*) as event_count
FROM event_store e
WHERE e.occurred_at BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY e.event_type
ORDER BY event_count DESC;

-- Get aggregate versions at end of each month (2024)
WITH monthly_snapshots AS (
    SELECT 
        e.aggregate_id,
        DATE_TRUNC('month', e.occurred_at) as month,
        MAX(e.sequence_number) as version_at_month_end
    FROM event_store e
    WHERE e.occurred_at >= '2024-01-01'
      AND e.occurred_at < '2025-01-01'
    GROUP BY e.aggregate_id, DATE_TRUNC('month', e.occurred_at)
)
SELECT * FROM monthly_snapshots
ORDER BY aggregate_id, month;
```

### 25.6 Performance Optimization - Temporal Index

```sql
-- Index dla temporal queries
CREATE INDEX idx_event_store_temporal 
    ON event_store(aggregate_id, occurred_at, sequence_number);

-- Partitioning by time (PostgreSQL)
CREATE TABLE event_store_2024_q1 PARTITION OF event_store
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE event_store_2024_q2 PARTITION OF event_store
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
```

### 25.7 Testowanie Temporal Queries

```java
@SpringBootTest
@Testcontainers
class TemporalQueriesTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18");
    
    @Autowired
    private TemporalEventRepository temporalRepo;
    
    @Autowired
    private EventSourcedEventRepository eventRepo;
    
    @Test
    void shouldReturnStateAtPointInTime() {
        // Given - event with state changes over time
        UUID eventId = UUID.randomUUID();
        
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        Instant t1 = Instant.parse("2024-06-01T00:00:00Z");
        Instant t2 = Instant.parse("2024-12-01T00:00:00Z");
        
        // t0: Event created (DRAFT)
        Event event = Event.create(eventId, "Test", "Desc", "Loc", 
            LocalDateTime.ofInstant(t0, ZoneOffset.UTC));
        eventRepo.save(event);
        
        // t1: Event published
        event.publish();
        eventRepo.save(event);
        
        // t2: Event cancelled
        event.cancel();
        eventRepo.save(event);
        
        // When / Then - query at different times
        
        // At t0 + 1 day: DRAFT
        Event atT0 = temporalRepo.findByIdAtTime(
            eventId, 
            t0.plus(1, ChronoUnit.DAYS)
        ).orElseThrow();
        assertThat(atT0.getStatus()).isEqualTo(EventStatus.DRAFT);
        
        // At t1 + 1 day: PUBLISHED
        Event atT1 = temporalRepo.findByIdAtTime(
            eventId, 
            t1.plus(1, ChronoUnit.DAYS)
        ).orElseThrow();
        assertThat(atT1.getStatus()).isEqualTo(EventStatus.PUBLISHED);
        
        // At t2 + 1 day: CANCELLED
        Event atT2 = temporalRepo.findByIdAtTime(
            eventId, 
            t2.plus(1, ChronoUnit.DAYS)
        ).orElseThrow();
        assertThat(atT2.getStatus()).isEqualTo(EventStatus.CANCELLED);
    }
    
    @Test
    void shouldNotExistBeforeCreation() {
        // Given - event created at t1
        UUID eventId = UUID.randomUUID();
        Instant t1 = Instant.parse("2024-06-01T00:00:00Z");
        
        Event event = Event.create(eventId, "Test", "Desc", "Loc",
            LocalDateTime.ofInstant(t1, ZoneOffset.UTC));
        eventRepo.save(event);
        
        // When - query before creation time
        Optional<Event> beforeCreation = temporalRepo.findByIdAtTime(
            eventId,
            t1.minus(1, ChronoUnit.DAYS)
        );
        
        // Then - doesn't exist
        assertThat(beforeCreation).isEmpty();
    }
}
```

### 25.8 Podsumowanie Tematu 25

**Kluczowe Pojęcia:**
- Temporal Query = zapytanie w czasie
- Point-in-time reconstruction
- Audit trail analysis

**Use Cases:**
- Audyt finansowy
- Debugging
- Compliance
- Raportowanie historyczne

**Następny temat:** [Event Store Optimization](#temat-26-event-store-optimization)

---

## Temat 26: Event Store Optimization

### 26.1 Performance Challenges

Event Store rośnie w nieskończoność:
```
Month 1:    100,000 events
Month 6:    600,000 events
Month 12:  1,200,000 events
Month 24:  2,400,000 events
```

**Problemy:**
- Odczyt aggregatu z 10,000 events trwa długo
- Backup/restore zajmuje godziny
- Rebuild projection = dni

### 26.2 Optimization Strategy 1: Snapshots

**Snapshot** = zrzut stanu aggregate co N events.

```java
@Entity
@Table(name = "aggregate_snapshots")
@Data
public class AggregateSnapshot {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private UUID aggregateId;
    
    @Column(nullable = false)
    private String aggregateType;
    
    @Column(nullable = false)
    private Long version;  // Version at snapshot time
    
    @Type(JsonBinaryType.class)
    @Column(columnDefinition = "jsonb", nullable = false)
    private String state;  // Serialized aggregate
    
    @Column(nullable = false)
    private Instant createdAt;
}
```

**Snapshot Service:**
```java
@Service
@RequiredArgsConstructor
public class SnapshotService {
    
    private final SnapshotRepository snapshotRepo;
    private final ObjectMapper objectMapper;
    
    private static final int SNAPSHOT_INTERVAL = 100;
    
    /**
     * Create snapshot if needed
     */
    public void createSnapshotIfNeeded(Event event) {
        if (event.getVersion() % SNAPSHOT_INTERVAL == 0) {
            createSnapshot(event);
        }
    }
    
    /**
     * Create snapshot
     */
    public void createSnapshot(Event event) {
        try {
            String state = objectMapper.writeValueAsString(event);
            
            AggregateSnapshot snapshot = new AggregateSnapshot();
            snapshot.setAggregateId(event.getId());
            snapshot.setAggregateType("Event");
            snapshot.setVersion(event.getVersion());
            snapshot.setState(state);
            snapshot.setCreatedAt(Instant.now());
            
            snapshotRepo.save(snapshot);
            
            log.info("Snapshot created for {} at version {}", 
                event.getId(), event.getVersion());
                
        } catch (Exception e) {
            log.error("Failed to create snapshot", e);
        }
    }
    
    /**
     * Load with snapshot optimization
     */
    public Optional<Event> loadWithSnapshot(UUID eventId) {
        // 1. Load latest snapshot
        Optional<AggregateSnapshot> snapshotOpt = 
            snapshotRepo.findLatestByAggregateId(eventId);
        
        if (snapshotOpt.isEmpty()) {
            // No snapshot, load all events
            return loadFromEvents(eventId, 0L);
        }
        
        AggregateSnapshot snapshot = snapshotOpt.get();
        
        // 2. Deserialize snapshot
        Event event = deserializeSnapshot(snapshot);
        
        // 3. Load events after snapshot
        List<DomainEvent> events = eventStore.loadEventsFromVersion(
            eventId,
            snapshot.getVersion()
        );
        
        // 4. Apply events to snapshot
        events.forEach(event::apply);
        
        return Optional.of(event);
    }
}
```

**Performance Improvement:**
```
Bez snapshot:
  Load 10,000 events → replay all → 5 seconds

Ze snapshot (co 100 events):
  Load snapshot at 9,900 → load 100 events → replay 100 → 0.1 seconds
  
50x szybciej! 🚀
```

### 26.3 Optimization Strategy 2: Event Compaction

**Compaction** = usuwanie nieistotnych events.

```java
@Service
@RequiredArgsConstructor
public class EventCompactionService {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Compact events for cancelled aggregates
     */
    @Scheduled(cron = "0 3 * * * *")  // 3 AM daily
    @Transactional
    public void compactCancelledEvents() {
        // Find cancelled events
        List<UUID> cancelledEventIds = eventStore.findCancelledAggregates();
        
        for (UUID eventId : cancelledEventIds) {
            // Keep only: EventCreated + EventCancelled
            // Remove all intermediate events
            compactAggregate(eventId);
        }
    }
    
    private void compactAggregate(UUID aggregateId) {
        List<DomainEvent> events = eventStore.loadEvents(aggregateId);
        
        // Filter: keep first and last only
        List<DomainEvent> compacted = List.of(
            events.get(0),  // EventCreated
            events.get(events.size() - 1)  // EventCancelled
        );
        
        // Delete all events
        eventStore.deleteAllForAggregate(aggregateId);
        
        // Re-append compacted events
        eventStore.appendEvents(aggregateId, "Event", 0L, compacted, metadata());
        
        log.info("Compacted {} events to {} for aggregate {}",
            events.size(), compacted.size(), aggregateId);
    }
}
```

**⚠️ Warning:** Compaction traci historię! Używaj ostrożnie.

### 26.4 Optimization Strategy 3: Archiving

**Archiving** = przeniesienie starych events do cold storage.

```sql
-- Active Event Store (PostgreSQL)
CREATE TABLE event_store (
    -- Recent events (last 12 months)
);

-- Archive Event Store (S3/cheaper storage)
CREATE TABLE event_store_archive (
    -- Events older than 12 months
);
```

```java
@Service
@RequiredArgsConstructor
public class EventArchivingService {
    
    private final EventStoreRepository activeStore;
    private final EventArchiveRepository archiveStore;
    
    @Scheduled(cron = "0 4 1 * * *")  // 4 AM, 1st day of month
    @Transactional
    public void archiveOldEvents() {
        Instant cutoffDate = Instant.now().minus(365, ChronoUnit.DAYS);
        
        // Find old events
        List<StoredEvent> oldEvents = activeStore.findOlderThan(cutoffDate);
        
        log.info("Archiving {} events older than {}", oldEvents.size(), cutoffDate);
        
        // Move to archive
        for (StoredEvent event : oldEvents) {
            archiveStore.save(event);
            activeStore.delete(event);
        }
        
        log.info("Archived {} events", oldEvents.size());
    }
}
```

### 26.5 Optimization Strategy 4: Indexing

```sql
-- Composite index dla aggregate queries
CREATE INDEX idx_event_store_aggregate_seq 
    ON event_store(aggregate_id, sequence_number);

-- Partial index dla active aggregates
CREATE INDEX idx_event_store_active 
    ON event_store(aggregate_id, sequence_number)
    WHERE occurred_at > CURRENT_DATE - INTERVAL '6 months';

-- Index dla temporal queries
CREATE INDEX idx_event_store_temporal 
    ON event_store(occurred_at, aggregate_id);

-- BRIN index dla time-series data
CREATE INDEX idx_event_store_time_brin 
    ON event_store USING brin(occurred_at);
```

### 26.6 Optimization Strategy 5: Partitioning

```sql
-- Partition by time (PostgreSQL 11+)
CREATE TABLE event_store (
    id BIGSERIAL,
    aggregate_id UUID NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    -- ...
) PARTITION BY RANGE (occurred_at);

-- Q1 2024
CREATE TABLE event_store_2024_q1 PARTITION OF event_store
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

-- Q2 2024
CREATE TABLE event_store_2024_q2 PARTITION OF event_store
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

-- Q3 2024
CREATE TABLE event_store_2024_q3 PARTITION OF event_store
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');

-- Q4 2024
CREATE TABLE event_store_2024_q4 PARTITION OF event_store
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
```

**Zalety:**
- Szybsze zapytania (query tylko aktywne partycje)
- Łatwiejszy archiving (DROP partition)
- Lepszy vacuum/analyze

### 26.7 Performance Monitoring

```java
@Service
@RequiredArgsConstructor
public class EventStoreMetrics {
    
    private final MeterRegistry registry;
    private final EventStoreRepository eventStore;
    
    @Scheduled(fixedDelay = 60000)  // Every minute
    public void recordMetrics() {
        // Total events
        long totalEvents = eventStore.count();
        registry.gauge("eventstore.total_events", totalEvents);
        
        // Events per aggregate type
        Map<String, Long> byType = eventStore.countByAggregateType();
        byType.forEach((type, count) -> 
            registry.gauge("eventstore.events_by_type", 
                Tags.of("type", type), count)
        );
        
        // Average events per aggregate
        double avgEventsPerAggregate = eventStore.getAverageEventsPerAggregate();
        registry.gauge("eventstore.avg_events_per_aggregate", avgEventsPerAggregate);
        
        // Largest aggregates
        List<AggregateStats> largest = eventStore.getLargestAggregates(10);
        largest.forEach(stats -> 
            log.info("Large aggregate: {} with {} events", 
                stats.getAggregateId(), stats.getEventCount())
        );
    }
}
```

### 26.8 Podsumowanie Tematu 26

**Optimization Strategies:**
1. ✅ **Snapshots** - co 100 events (50x szybciej)
2. ✅ **Event Compaction** - usuń nieistotne events
3. ✅ **Archiving** - stare events do cold storage
4. ✅ **Indexing** - composite, partial, BRIN
5. ✅ **Partitioning** - by time/aggregate type

**Trade-offs:**
- Snapshots = więcej storage, szybsze odczyty
- Compaction = mniej storage, utrata historii
- Archiving = compliance, dwupoziomowy access

**Następny temat:** [GDPR & Event Sourcing](#temat-27-gdpr-event-sourcing)

---

## Temat 27: GDPR & Event Sourcing

### 27.1 Problem: Right to be Forgotten

GDPR Artykuł 17: **"Prawo do usunięcia danych"**

```
User: "Usuńcie moje dane!"
Event Store: "Ale... events są immutable! 😱"
```

**Konflikt:**
- Event Sourcing = immutable events
- GDPR = prawo do usunięcia

### 27.2 Solutions

#### Solution 1: Crypto Shredding

**Idea:** Szyfruj dane osobowe kluczem per-user. Usuń klucz = dane nieczytelne.

```java
// Event z zaszyfrowanymi danymi
public record BookingCreatedEvent(
    UUID bookingId,
    String encryptedUserData,  // Encrypted!
    UUID userKeyId             // Reference to encryption key
) {}

// Encryption Service
@Service
@RequiredArgsConstructor
public class GDPREncryptionService {
    
    private final UserKeyRepository keyRepository;
    
    /**
     * Encrypt personal data
     */
    public String encrypt(String data, UUID userId) {
        UserEncryptionKey key = keyRepository.findByUserId(userId)
            .orElseGet(() -> generateKey(userId));
        
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key.getSecretKey());
            
            byte[] encrypted = cipher.doFinal(data.getBytes(UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new EncryptionException("Encryption failed", e);
        }
    }
    
    /**
     * Decrypt personal data
     */
    public String decrypt(String encryptedData, UUID userKeyId) {
        UserEncryptionKey key = keyRepository.findById(userKeyId)
            .orElseThrow(() -> new KeyNotFoundException("Key deleted (GDPR)"));
        
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedData);
            
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key.getSecretKey());
            
            byte[] decrypted = cipher.doFinal(decoded);
            return new String(decrypted, UTF_8);
        } catch (Exception e) {
            throw new DecryptionException("Decryption failed", e);
        }
    }
    
    /**
     * Delete user key (makes data unreadable)
     */
    @Transactional
    public void deleteUserKey(UUID userId) {
        keyRepository.deleteByUserId(userId);
        log.info("User key deleted (GDPR): {}", userId);
    }
}
```

**Event Store:**
```sql
-- Event contains encrypted data
INSERT INTO event_store (payload) VALUES (
    '{
        "bookingId": "...",
        "encryptedUserData": "xJ8aK92...",  -- Encrypted!
        "userKeyId": "..."
    }'
);

-- User requests deletion
-- → Delete encryption key
DELETE FROM user_encryption_keys WHERE user_id = ?;

-- Event still exists, but data is unreadable! 🔒
SELECT payload FROM event_store;
-- → Cannot decrypt without key
```

#### Solution 2: Pseudonymization

**Idea:** Zastąp dane osobowe pseudonimem.

```java
// Before GDPR request
public record EventCreatedEvent(
    UUID eventId,
    String organizerName,  // "John Doe"
    String organizerEmail  // "john@example.com"
) {}

// After GDPR request
public record EventCreatedEvent(
    UUID eventId,
    String organizerName,  // "DELETED_USER_12345"
    String organizerEmail  // "deleted@gdpr.local"
) {}
```

**Implementation:**
```java
@Service
@RequiredArgsConstructor
public class GDPRPseudonymizationService {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Pseudonymize user data in all events
     */
    @Transactional
    public void pseudonymizeUser(UUID userId) {
        // Find all events containing user data
        List<StoredEvent> events = eventStore.findByUserId(userId);
        
        log.info("Pseudonymizing {} events for user {}", events.size(), userId);
        
        for (StoredEvent event : events) {
            // Parse payload
            JsonNode payload = objectMapper.readTree(event.getPayload());
            
            // Replace personal data
            ((ObjectNode) payload).put("userName", "DELETED_USER_" + userId);
            ((ObjectNode) payload).put("userEmail", "deleted@gdpr.local");
            ((ObjectNode) payload).put("phoneNumber", "DELETED");
            
            // Update event
            event.setPayload(payload.toString());
            eventStore.save(event);
        }
        
        log.info("Pseudonymization completed for user {}", userId);
    }
}
```

**⚠️ Warning:** To narusza immutability! Używaj ostrożnie.

#### Solution 3: Forget Event

**Idea:** Dodaj nowy event `UserDataDeletedEvent`.

```java
public record UserDataDeletedEvent(
    UUID userId,
    Instant deletedAt,
    String reason  // "GDPR Article 17"
) implements DomainEvent {}

// Projector obsługuje ten event
@EventHandler
public void handle(UserDataDeletedEvent event) {
    // Remove user from Read Models
    userViewRepository.deleteByUserId(event.userId());
    bookingViewRepository.pseudonymizeByUserId(event.userId());
}
```

**Event Store:**
```sql
-- Original events remain
id=1: UserRegisteredEvent
id=2: BookingCreatedEvent
-- ...
id=100: UserDataDeletedEvent  -- New event!

-- Projectors see UserDataDeletedEvent and remove/pseudonymize data
```

### 27.3 Compliance Table

| Solution | GDPR Compliant? | Immutability | Complexity |
|----------|----------------|--------------|------------|
| **Crypto Shredding** | ✅ Yes | ✅ Preserved | Medium |
| **Pseudonymization** | ⚠️ Partial | ❌ Violated | Low |
| **Forget Event** | ✅ Yes | ✅ Preserved | Low |

**Recommended:** Crypto Shredding + Forget Event

### 27.4 Testing GDPR Compliance

```java
@SpringBootTest
class GDPRComplianceTest {
    
    @Autowired
    private GDPREncryptionService encryptionService;
    
    @Autowired
    private UserKeyRepository keyRepository;
    
    @Test
    void shouldMakeDataUnreadableAfterKeyDeletion() {
        // Given - user data encrypted
        UUID userId = UUID.randomUUID();
        String personalData = "John Doe, john@example.com, +48 123 456 789";
        
        String encrypted = encryptionService.encrypt(personalData, userId);
        UUID keyId = keyRepository.findByUserId(userId).get().getId();
        
        // When - user requests deletion
        encryptionService.deleteUserKey(userId);
        
        // Then - data cannot be decrypted
        assertThatThrownBy(() -> 
            encryptionService.decrypt(encrypted, keyId)
        ).isInstanceOf(KeyNotFoundException.class)
          .hasMessageContaining("Key deleted (GDPR)");
    }
    
    @Test
    void shouldPreserveEventStoreIntegrity() {
        // Given - events with encrypted data
        UUID userId = UUID.randomUUID();
        createEventsWithUserData(userId);
        
        long eventCountBefore = eventStore.count();
        
        // When - GDPR deletion
        encryptionService.deleteUserKey(userId);
        
        // Then - Event Store unchanged
        long eventCountAfter = eventStore.count();
        assertThat(eventCountAfter).isEqualTo(eventCountBefore);
    }
}
```

### 27.5 Podsumowanie Tematu 27

**Solutions:**
1. ✅ **Crypto Shredding** - szyfruj + usuń klucz
2. ⚠️ **Pseudonymization** - zamień dane (narusza immutability)
3. ✅ **Forget Event** - nowy event + cleanup w projections

**Best Practice:** Crypto Shredding + minimal personal data in events

---

## Temat 28: Event Compaction

### 28.1 Problem: Nieskończony Wzrost

```
Aggregate z 10,000 events:
- EventCreated
- NameChanged (x 500)
- DescriptionChanged (x 1000)
- ...
- EventCancelled

Tylko ostatni stan jest istotny dla CANCELLED event!
```

### 28.2 Compaction Strategies

#### Strategy 1: Keep First + Last

```java
@Service
public class SimpleCompactionService {
    
    @Transactional
    public void compactAggregate(UUID aggregateId) {
        List<DomainEvent> events = eventStore.loadEvents(aggregateId);
        
        if (events.size() < 100) {
            return;  // Not worth compacting
        }
        
        // Keep: EventCreated + last event
        List<DomainEvent> compacted = List.of(
            events.get(0),
            events.get(events.size() - 1)
        );
        
        // Replace
        eventStore.replaceEvents(aggregateId, compacted);
        
        log.info("Compacted {} → {} events", events.size(), compacted.size());
    }
}
```

#### Strategy 2: Snapshot + Recent Events

```java
@Service
public class SnapshotBasedCompactionService {
    
    @Transactional
    public void compactAggregate(UUID aggregateId) {
        // 1. Create snapshot
        Event aggregate = loadAggregate(aggregateId);
        snapshotService.createSnapshot(aggregate);
        
        // 2. Delete events older than snapshot - 100
        Long snapshotVersion = aggregate.getVersion();
        Long keepFromVersion = snapshotVersion - 100;
        
        eventStore.deleteEventsBefore(aggregateId, keepFromVersion);
        
        log.info("Compacted: kept events from version {}", keepFromVersion);
    }
}
```

#### Strategy 3: Event Replay Optimization

```java
@Service
public class SmartCompactionService {
    
    @Transactional
    public void compactAggregate(UUID aggregateId) {
        List<DomainEvent> events = eventStore.loadEvents(aggregateId);
        
        // Apply events to get final state
        Event finalState = Event.from(events);
        
        // Create single "CompactedEvent" with final state
        EventCompactedEvent compacted = new EventCompactedEvent(
            aggregateId,
            finalState.toSnapshot(),
            events.size(),
            Instant.now()
        );
        
        // Replace all events with single compacted event
        eventStore.replaceEvents(aggregateId, List.of(compacted));
    }
}
```

### 28.3 Podsumowanie Tematu 28

**Compaction = reduce number of events**

**Use Cases:**
- Cancelled aggregates
- Long-lived aggregates (10,000+ events)
- Archived data

**⚠️ Warning:** Traci historię! Dokumentuj przed kompresją.

---

## Temat 29: Debugging with Events

### 29.1 Event Store jako Time Machine

Event Sourcing = najlepsze narzędzie debugowania!

```java
// User report: "Bug occurred on 2024-12-15"

// 1. Load state at that time
Event stateAtBug = temporalRepo.findByIdAtTime(
    eventId,
    Instant.parse("2024-12-15T10:00:00Z")
);

// 2. Replay events step by step
List<DomainEvent> events = eventStore.loadEvents(eventId);
Event state = new Event();

for (DomainEvent event : events) {
    state.apply(event);
    log.info("After {}: state={}", event.getClass(), state);
    
    // Debug breakpoint here! 🐛
}
```

### 29.2 Event Replay Debugger

```java
@RestController
@RequestMapping("/api/debug")
@RequiredArgsConstructor
public class EventDebugController {
    
    private final EventStoreRepository eventStore;
    
    /**
     * Replay events with detailed logging
     */
    @GetMapping("/replay/{aggregateId}")
    public ReplayResult replayEvents(@PathVariable UUID aggregateId) {
        List<DomainEvent> events = eventStore.loadEvents(aggregateId);
        
        List<StateSnapshot> snapshots = new ArrayList<>();
        Event state = new Event();
        
        for (DomainEvent event : events) {
            // Snapshot before
            StateSnapshot before = new StateSnapshot(state);
            
            // Apply event
            state.apply(event);
            
            // Snapshot after
            StateSnapshot after = new StateSnapshot(state);
            
            snapshots.add(new StateTransition(event, before, after));
        }
        
        return new ReplayResult(aggregateId, snapshots);
    }
}
```

### 29.3 Podsumowanie Tematu 29

**Event Sourcing = Best Debugging Tool:**
- ✅ Full history
- ✅ Step-by-step replay
- ✅ Time travel queries
- ✅ Root cause analysis

---

## Temat 30: Hybrid Approach - Łączenie Stylów

### 30.1 Problem: Not Everything Needs Event Sourcing

Event Sourcing ma koszty:
- Complexity ⬆️
- Storage ⬆️
- Learning curve ⬆️

**Nie wszystko tego wymaga!**

### 30.2 Decision Matrix

| Aggregate | Event Sourcing? | Reason |
|-----------|----------------|--------|
| **Booking** | ✅ Yes | Audit trail crucial |
| **Payment** | ✅ Yes | Financial compliance |
| **Event** | ✅ Yes | Complex lifecycle |
| **User** | ❌ No | Simple CRUD |
| **Category** | ❌ No | Simple CRUD |
| **Tag** | ❌ No | Simple CRUD |
| **Comment** | ❌ No | Append-only anyway |

### 30.3 Hybrid Architecture

```
┌────────────────────────────────────────┐
│  Event Sourcing (Critical Aggregates)  │
│  ┌──────────┐  ┌──────────┐           │
│  │ Booking  │  │ Payment  │           │
│  └──────────┘  └──────────┘           │
│       ↓              ↓                 │
│  [Event Store (PostgreSQL)]           │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Traditional (Simple Entities)         │
│  ┌──────────┐  ┌──────────┐           │
│  │   User   │  │ Category │           │
│  └──────────┘  └──────────┘           │
│       ↓              ↓                 │
│  [Regular Tables (PostgreSQL)]        │
└────────────────────────────────────────┘
```

### 30.4 Implementacja Hybrid

```java
// Event Sourced Aggregate
@Component
public class BookingRepository {
    
    private final EventStoreRepository eventStore;
    
    public void save(Booking booking) {
        eventStore.appendEvents(
            booking.getId(),
            "Booking",
            booking.getVersion(),
            booking.getUncommittedEvents(),
            metadata()
        );
    }
}

// Traditional Aggregate
@Repository
public interface UserRepository extends JpaRepository<User, UUID> {
    // Standard JPA - simple CRUD
}
```

### 30.5 Podsumowanie Tematu 30

**Hybrid = Best of Both Worlds:**
- Event Sourcing dla krytycznych agregat
- Traditional persistence dla prostych entities

**Decision Criteria:**
- Audit trail needed? → Event Sourcing
- Complex state machine? → Event Sourcing
- Simple CRUD? → Traditional
- Performance > history? → Traditional

---

## 🎓 Podsumowanie CZĘŚĆ III

### ✅ Ukończone Tematy (21-30):

21. **Event Sourcing - Wprowadzenie** - fundamenty persystencji przez events
22. **Event Store Implementation** - PostgreSQL z JSONB, checksums, metadata
23. **Event Versioning** - strategie: additive, new type, upcasting
24. **Upcasting Events** - transformacja V1 → V2 → V3
25. **Temporal Queries** - point-in-time queries, audit trail
26. **Event Store Optimization** - snapshots, compaction, archiving, indexing
27. **GDPR & Event Sourcing** - crypto shredding, pseudonymization, forget events
28. **Event Compaction** - redukcja liczby events
29. **Debugging with Events** - time machine, replay debugger
30. **Hybrid Approach** - łączenie Event Sourcing z traditional persistence

### 🎯 Zdobyte Umiejętności:

- ✅ Implementacja pełnego Event Store w PostgreSQL 18
- ✅ Event versioning i upcasting
- ✅ Temporal queries (time travel!)
- ✅ Optymalizacja Event Store (snapshots, compaction)
- ✅ GDPR compliance w Event Sourcing
- ✅ Debugging z Event Store
- ✅ Hybrid architecture design

### 📊 Statystyki CZĘŚĆ III:

```
Tematy:                        10
Linie kodu przykładowego:    150+
Strategie optymalizacji:       5
GDPR solutions:                3
Performance improvements:   50x (snapshots)
```

### 🛠️ Stack Technologiczny:

- **PostgreSQL 18** - Event Store z JSONB, partitioning
- **JPA/Hibernate** - Entity mapping, optimistic locking
- **Jackson** - JSON serialization/deserialization
- **AES-256** - Crypto Shredding (GDPR)
- **Testcontainers** - Integration tests

### 🚀 Kiedy Używać Event Sourcing?

**✅ UŻYJ gdy:**
- Audit trail jest kluczowy
- Potrzebujesz temporal queries
- Złożone state machines
- Financial/medical domain
- Debugging jest priorytetem

**❌ NIE UŻYWAJ gdy:**
- Prosty CRUD
- Performance > completeness
- Brak wymagań audytowych
- Zespół bez doświadczenia
- Prototypowanie

### 📈 Performance Guidelines:

```
Bez optymalizacji:
- 10,000 events → 5 seconds load

Ze Snapshots (co 100):
- 100 events → 0.1 second load
- 50x szybciej! 🚀

Z Compaction:
- 10,000 → 2 events
- 5000x mniej storage! 💾
```

### 🔒 GDPR Compliance:

**Recommended Stack:**
1. **Crypto Shredding** - encrypt personal data per-user
2. **Forget Event** - UserDataDeletedEvent
3. **Minimal PII** - store only necessary personal data

```java
// GOOD - encrypted
public record BookingCreatedEvent(
    UUID bookingId,
    String encryptedUserData,
    UUID userKeyId
) {}

// BAD - plain personal data
public record BookingCreatedEvent(
    UUID bookingId,
    String userName,      // PII!
    String userEmail,     // PII!
    String phoneNumber    // PII!
) {}
```

### 🎓 Następne Kroki:

**Dla Projektu EventMaster:**
1. Implementuj Event Store w PostgreSQL 18
2. Dodaj Event Sourcing dla Booking
3. Zaimplementuj Snapshots
4. Dodaj GDPR crypto shredding
5. Utwórz temporal query API

**Dla Dalszej Nauki (opcjonalne):**
- CZĘŚĆ IV: Messaging & Kafka (Tematy 31-40)
- CZĘŚĆ V: Testowanie & Operacje (Tematy 41-50)

---

## 📚 Dodatkowe Zasoby

### Książki (Event Sourcing):
1. **"Versioning in an Event Sourced System"** - Greg Young
2. **"Event Sourcing Distilled"** - Mathias Verraes
3. **"Patterns, Principles, and Practices of Domain-Driven Design"** - Scott Millett

### Artykuły:
- [Martin Fowler - Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Greg Young - CQRS & Event Sourcing](https://cqrs.wordpress.com/)
- [Axon Framework Documentation](https://docs.axoniq.io/)

### Narzędzia:
- **EventStoreDB** - Dedicated Event Store
- **Axon Framework** - Java framework dla CQRS/ES
- **Marten** - Event Store dla .NET

---

**Dokument ukończony:** 2025-01-20  
**Wersja:** 1.0  
**Status:** ✅ KOMPLETNA  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4  
**Autorzy:** EventMaster Architecture Team

---

**🎉 Gratulacje! Ukończyłeś CZĘŚĆ III: Event Sourcing! 🎉**

**Teraz znasz:**
- ✅ Event Store implementation
- ✅ Event versioning & upcasting
- ✅ Temporal queries
- ✅ Performance optimization
- ✅ GDPR compliance
- ✅ Debugging with events
- ✅ Hybrid approaches

**🚀 Gotowy do implementacji w EventMaster! 🚀**

