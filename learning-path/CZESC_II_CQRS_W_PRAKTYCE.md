# 🎓 CZĘŚĆ II: CQRS w Praktyce (Tematy 11-20)

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Apache Kafka + Spring Boot 3.4 + Nuxt 3  
**Cel:** Praktyczne implementacje wzorców CQRS  
**Czas nauki:** 3-4 tygodnie  
**Wymagania:** Ukończona [CZĘŚĆ I: Fundamenty](./CZESC_I_FUNDAMENTY.md)

---

## 📋 Spis Treści - CZĘŚĆ II

11. [Projections - Budowanie Read Model](#temat-11-projections)
12. [Multiple Read Models](#temat-12-multiple-read-models)
13. [Rebuilding Projections](#temat-13-rebuilding-projections)
14. [Optimistic Concurrency Control](#temat-14-optimistic-concurrency-control)
15. [Command Validation](#temat-15-command-validation)
16. [Command Handler Pattern](#temat-16-command-handler-pattern)
17. [Query Handler Pattern](#temat-17-query-handler-pattern)
18. [Denormalization](#temat-18-denormalization)
19. [Materialized Views](#temat-19-materialized-views)
20. [Snapshot Pattern](#temat-20-snapshot-pattern)

---

## 🎯 Cele Nauki - CZĘŚĆ II

Po ukończeniu Części II będziesz:
- ✅ Implementować projectors dla Read Models
- ✅ Tworzyć wiele zoptymalizowanych Read Models
- ✅ Odbudowywać Read Models z Event Stream
- ✅ Stosować Optimistic Locking
- ✅ Walidować Commands na wielu poziomach
- ✅ Projektować Command i Query Handlers
- ✅ Denormalizować dane dla wydajności
- ✅ Używać Materialized Views w PostgreSQL
- ✅ Implementować Snapshot Pattern

---

## Temat 11: Projections - Budowanie Read Model

### 11.1 Definicja

**Projection** to proces przekształcania Event Stream w Read Model zoptymalizowany pod konkretne zapytania.

```
Event Stream (Kafka)          Projector              Read Model (PostgreSQL)
─────────────────────        ─────────────        ───────────────────────────
EventCreatedEvent       →                    →    EventView (denormalized)
EventPublishedEvent     →    Transform       →    Fast queries, no JOINs
EventCancelledEvent     →                    →    Optimized indexes
```

### 11.2 Rodzaje Projections

#### 1. Simple Projection (1:1 mapping)

```java
EventCreatedEvent → EventView
```

#### 2. Aggregating Projection (N:1 mapping)

```java
EventCreatedEvent   ↘
TicketAddedEvent     ↓  EventStatisticsView
TicketSoldEvent      ↓  (aggregated data)
BookingCreatedEvent ↗
```

#### 3. Filtering Projection (N:M mapping)

```java
EventCreatedEvent → if(status == PUBLISHED) → PublishedEventsView
```

### 11.3 Implementacja - Simple Projector

**backend/src/main/java/com/eventmaster/query/projector/EventViewProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventViewProjector {
    
    private final EventViewRepository repository;
    private final ProcessedEventRepository processedRepo;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting EventCreatedEvent: {}", event.eventId());
        
        // Idempotency check
        if (processedRepo.existsByEventIdAndType(
            event.eventId(), 
            "EventCreatedEvent"
        )) {
            log.warn("Event already processed, skipping: {}", event.eventId());
            return;
        }
        
        // Transform event → view
        EventView view = new EventView();
        view.setId(event.eventId());
        view.setName(event.name());
        view.setShortDescription(truncate(event.description(), 200));
        view.setLocation(event.location());
        view.setStartDate(event.startDate());
        view.setFormattedDate(formatDate(event.startDate()));
        view.setStatus("DRAFT");
        view.setIsPublished(false);
        view.setCreatedAt(event.occurredAt());
        view.setUpdatedAt(event.occurredAt());
        
        // Save
        repository.save(view);
        
        // Mark as processed
        processedRepo.save(new ProcessedEvent(
            event.eventId(),
            "EventCreatedEvent",
            Instant.now()
        ));
        
        log.info("EventView projected successfully: {}", view.getId());
    }
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors"
    )
    @Transactional
    public void onEventPublished(EventPublishedEvent event) {
        log.info("Projecting EventPublishedEvent: {}", event.eventId());
        
        // Idempotency
        if (processedRepo.existsByEventIdAndType(
            event.eventId(), 
            "EventPublishedEvent"
        )) {
            log.warn("Event already processed, skipping");
            return;
        }
        
        // Update existing view
        EventView view = repository.findById(event.eventId())
            .orElseThrow(() -> new ProjectionException(
                "EventView not found for EventPublishedEvent: " + event.eventId()
            ));
        
        view.setStatus("PUBLISHED");
        view.setIsPublished(true);
        view.setUpdatedAt(event.occurredAt());
        
        repository.save(view);
        
        processedRepo.save(new ProcessedEvent(
            event.eventId(),
            "EventPublishedEvent",
            Instant.now()
        ));
        
        log.info("EventView updated: status=PUBLISHED");
    }
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-view-projectors"
    )
    @Transactional
    public void onEventCancelled(EventCancelledEvent event) {
        log.info("Projecting EventCancelledEvent: {}", event.eventId());
        
        if (processedRepo.existsByEventIdAndType(
            event.eventId(), 
            "EventCancelledEvent"
        )) {
            return;
        }
        
        EventView view = repository.findById(event.eventId())
            .orElseThrow(() -> new ProjectionException(
                "EventView not found: " + event.eventId()
            ));
        
        view.setStatus("CANCELLED");
        view.setIsPublished(false);
        view.setUpdatedAt(event.occurredAt());
        
        repository.save(view);
        
        processedRepo.save(new ProcessedEvent(
            event.eventId(),
            "EventCancelledEvent",
            Instant.now()
        ));
    }
    
    private String truncate(String text, int maxLength) {
        if (text == null) return null;
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength - 3) + "...";
    }
    
    private String formatDate(LocalDateTime date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
            "dd MMM yyyy, HH:mm",
            Locale.ENGLISH
        );
        return date.format(formatter);
    }
}
```

### 11.4 Implementacja - Aggregating Projector

**backend/src/main/java/com/eventmaster/query/projector/EventStatisticsProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventStatisticsProjector {
    
    private final EventStatisticsRepository repository;
    
    @KafkaListener(topics = "events.lifecycle")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        // Initialize statistics
        EventStatistics stats = new EventStatistics();
        stats.setEventId(event.eventId());
        stats.setViewCount(0L);
        stats.setBookingCount(0L);
        stats.setRevenue(BigDecimal.ZERO);
        stats.setConversionRate(BigDecimal.ZERO);
        
        repository.save(stats);
    }
    
    @KafkaListener(topics = "events.tracking")
    @Transactional
    public void onEventViewed(EventViewedEvent event) {
        // Increment view count
        EventStatistics stats = repository.findByEventId(event.eventId())
            .orElseThrow();
        
        stats.setViewCount(stats.getViewCount() + 1);
        stats.updateConversionRate();
        
        repository.save(stats);
    }
    
    @KafkaListener(topics = "bookings.lifecycle")
    @Transactional
    public void onBookingCreated(BookingCreatedEvent event) {
        // Increment booking count
        EventStatistics stats = repository.findByEventId(event.eventId())
            .orElseThrow();
        
        stats.setBookingCount(stats.getBookingCount() + 1);
        stats.setRevenue(stats.getRevenue().add(event.totalPrice()));
        stats.updateConversionRate();
        
        repository.save(stats);
    }
}

@Entity
@Table(name = "event_statistics")
@Data
public class EventStatistics {
    @Id
    private UUID eventId;
    
    private Long viewCount;
    private Long bookingCount;
    private BigDecimal revenue;
    private BigDecimal conversionRate;  // bookingCount / viewCount
    
    public void updateConversionRate() {
        if (viewCount == 0) {
            this.conversionRate = BigDecimal.ZERO;
            return;
        }
        
        this.conversionRate = BigDecimal.valueOf(bookingCount)
            .divide(BigDecimal.valueOf(viewCount), 4, RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));  // Percentage
    }
}
```

### 11.5 Testowanie Projectors

#### Test 1: Simple Projection

```java
@SpringBootTest
@Testcontainers
class EventViewProjectorTest {
    
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
    private EventViewRepository repository;
    
    @Test
    void shouldProjectEventCreatedToEventView() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "Projection Test Event",
            "This is a long description that will be truncated to 200 chars...",
            "Berlin, Germany",
            LocalDateTime.of(2025, 7, 15, 10, 0),
            Instant.now()
        );
        
        // When
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        
        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> {
            Optional<EventView> viewOpt = repository.findById(event.eventId());
            assertThat(viewOpt).isPresent();
            
            EventView view = viewOpt.get();
            assertThat(view.getName()).isEqualTo("Projection Test Event");
            assertThat(view.getShortDescription()).hasSize(200);
            assertThat(view.getShortDescription()).endsWith("...");
            assertThat(view.getFormattedDate()).matches("\\d{2} \\w{3} \\d{4}, \\d{2}:\\d{2}");
            assertThat(view.getStatus()).isEqualTo("DRAFT");
            assertThat(view.getIsPublished()).isFalse();
        });
    }
    
    @Test
    void shouldUpdateViewOnEventPublished() {
        // Given - event już istnieje
        UUID eventId = UUID.randomUUID();
        EventCreatedEvent created = new EventCreatedEvent(
            eventId,
            "Test Event",
            "Description",
            "Location",
            LocalDateTime.now().plusDays(1),
            Instant.now()
        );
        
        eventPublisher.send("events.lifecycle", eventId.toString(), created);
        
        await().until(() -> repository.findById(eventId).isPresent());
        
        // When - publikuj event
        EventPublishedEvent published = new EventPublishedEvent(
            eventId,
            Instant.now()
        );
        
        eventPublisher.send("events.lifecycle", eventId.toString(), published);
        
        // Then - view zaktualizowany
        await().atMost(10, SECONDS).untilAsserted(() -> {
            EventView view = repository.findById(eventId).orElseThrow();
            assertThat(view.getStatus()).isEqualTo("PUBLISHED");
            assertThat(view.getIsPublished()).isTrue();
        });
    }
    
    @Test
    void shouldBeIdempotent() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(/* ... */);
        
        // When - publish 3 times
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        
        // Then - tylko 1 view
        await().atMost(10, SECONDS).untilAsserted(() -> {
            List<EventView> views = repository.findAll();
            assertThat(views).hasSize(1);
        });
    }
}
```

#### Test 2: Aggregating Projection

```java
@Test
void shouldAggregateMultipleEvents() {
    // Given
    UUID eventId = UUID.randomUUID();
    
    // Event created
    eventPublisher.send("events.lifecycle", new EventCreatedEvent(eventId, /* ... */));
    
    await().until(() -> statsRepository.findByEventId(eventId).isPresent());
    
    // When - multiple events
    eventPublisher.send("events.tracking", new EventViewedEvent(eventId));
    eventPublisher.send("events.tracking", new EventViewedEvent(eventId));
    eventPublisher.send("events.tracking", new EventViewedEvent(eventId));
    
    eventPublisher.send("bookings.lifecycle", 
        new BookingCreatedEvent(eventId, BigDecimal.valueOf(100)));
    
    // Then - statistics aggregated
    await().atMost(15, SECONDS).untilAsserted(() -> {
        EventStatistics stats = statsRepository.findByEventId(eventId).orElseThrow();
        
        assertThat(stats.getViewCount()).isEqualTo(3L);
        assertThat(stats.getBookingCount()).isEqualTo(1L);
        assertThat(stats.getRevenue()).isEqualByComparingTo("100.00");
        assertThat(stats.getConversionRate()).isEqualByComparingTo("33.33");  // 1/3 * 100
    });
}
```

### 11.6 Error Handling w Projectors

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ResilientEventViewProjector {
    
    private final EventViewRepository repository;
    private final DeadLetterQueueService dlqService;
    
    @KafkaListener(topics = "events.lifecycle")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        try {
            // Projection logic
            EventView view = project(event);
            repository.save(view);
            
        } catch (DataIntegrityViolationException e) {
            // Już istnieje - idempotency
            log.warn("Duplicate event, skipping: {}", event.eventId());
            
        } catch (Exception e) {
            // Nieoczekiwany błąd
            log.error("Projection failed for event: {}", event.eventId(), e);
            
            // Przenieś do Dead Letter Queue
            dlqService.send(event, e.getMessage());
            
            // Nie rzucaj wyjątku - nie blokuj innych eventów
        }
    }
}
```

### 11.7 Monitoring Projectors

```java
@Service
@RequiredArgsConstructor
public class MonitoredEventViewProjector {
    
    private final EventViewRepository repository;
    private final MeterRegistry meterRegistry;
    
    @KafkaListener(topics = "events.lifecycle")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            EventView view = project(event);
            repository.save(view);
            
            // Success metric
            meterRegistry.counter("projection.success", 
                "type", "EventView",
                "event_type", "EventCreatedEvent"
            ).increment();
            
        } catch (Exception e) {
            // Failure metric
            meterRegistry.counter("projection.failure",
                "type", "EventView",
                "event_type", "EventCreatedEvent",
                "error", e.getClass().getSimpleName()
            ).increment();
            
            throw e;
        } finally {
            sample.stop(meterRegistry.timer("projection.duration",
                "type", "EventView"
            ));
        }
    }
}
```

### 11.8 Podsumowanie Tematu 11

**Kluczowe Pojęcia:**
- Projection = Event Stream → Read Model
- Simple projection (1:1)
- Aggregating projection (N:1)
- Idempotency jest kluczowa
- Error handling = Dead Letter Queue

**Best Practices:**
- ✅ Zawsze sprawdzaj idempotentność
- ✅ Używaj transakcji (@Transactional)
- ✅ Loguj wszystkie operacje
- ✅ Monitoruj metryki (duration, success, failure)
- ✅ Obsługuj błędy gracefully (DLQ)

**Następny temat:** [Multiple Read Models](#temat-12-multiple-read-models)

---

## Temat 12: Multiple Read Models - Różne Widoki

### 12.1 Definicja

W CQRS możemy mieć **wiele Read Models** zoptymalizowanych pod różne use cases.

```
                    EventCreatedEvent
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ↓                  ↓                  ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ EventListView│  │EventDetailView│  │EventSearchView│
│              │  │               │  │              │
│ (Fast list)  │  │ (Full details)│  │(Full-text   │
│ - name       │  │ - name        │  │  search)     │
│ - date       │  │ - description │  │ - indexed    │
│ - city       │  │ - organizer   │  │   fields     │
└──────────────┘  └──────────────┘  └──────────────┘
```

### 12.2 Przypadki Użycia

#### 1. EventListView - Lista Wydarzeń

**Optymalizacja:** Szybkie wyświetlanie listy
```sql
CREATE TABLE event_list_view (
    id UUID PRIMARY KEY,
    name VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100),
    start_date TIMESTAMP,
    formatted_date VARCHAR(50),
    is_published BOOLEAN
);

CREATE INDEX idx_event_list_published ON event_list_view(is_published, start_date DESC);
```

#### 2. EventDetailView - Szczegóły Wydarzenia

**Optymalizacja:** Pełne informacje, zdenormalizowane
```sql
CREATE TABLE event_detail_view (
    id UUID PRIMARY KEY,
    name VARCHAR(200),
    full_description TEXT,
    location_full VARCHAR(500),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    organizer_name VARCHAR(200),
    organizer_email VARCHAR(200),
    ticket_types JSONB,  -- PostgreSQL JSON!
    total_capacity INTEGER,
    available_tickets INTEGER,
    tags TEXT[]
);
```

#### 3. EventSearchView - Wyszukiwanie Full-Text

**Optymalizacja:** PostgreSQL Full-Text Search
```sql
CREATE TABLE event_search_view (
    id UUID PRIMARY KEY,
    name VARCHAR(200),
    description TEXT,
    location VARCHAR(500),
    tags TEXT[],
    search_vector tsvector  -- Full-text search!
);

-- GIN index for full-text search
CREATE INDEX idx_event_search_fts ON event_search_view 
    USING gin(search_vector);

-- Trigger to update search_vector
CREATE TRIGGER event_search_vector_update 
BEFORE INSERT OR UPDATE ON event_search_view
FOR EACH ROW EXECUTE FUNCTION
tsvector_update_trigger(
    search_vector, 'pg_catalog.english', 
    name, description, location
);
```

### 12.3 Implementacja - Multiple Projectors

#### EventListViewProjector

**backend/src/main/java/com/eventmaster/query/projector/EventListViewProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventListViewProjector {
    
    private final EventListViewRepository repository;
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-list-view-projectors"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting to EventListView: {}", event.eventId());
        
        EventListView view = new EventListView();
        view.setId(event.eventId());
        view.setName(event.name());
        view.setCity(extractCity(event.location()));
        view.setCountry(extractCountry(event.location()));
        view.setStartDate(event.startDate());
        view.setFormattedDate(formatDate(event.startDate()));
        view.setIsPublished(false);
        
        repository.save(view);
    }
    
    @KafkaListener(topics = "events.lifecycle", groupId = "event-list-view-projectors")
    @Transactional
    public void onEventPublished(EventPublishedEvent event) {
        EventListView view = repository.findById(event.eventId()).orElseThrow();
        view.setIsPublished(true);
        repository.save(view);
    }
    
    private String extractCity(String location) {
        // "Berlin, Germany" → "Berlin"
        return location.split(",")[0].trim();
    }
    
    private String extractCountry(String location) {
        // "Berlin, Germany" → "Germany"
        String[] parts = location.split(",");
        return parts.length > 1 ? parts[1].trim() : "";
    }
    
    private String formatDate(LocalDateTime date) {
        return date.format(DateTimeFormatter.ofPattern("dd MMM yyyy"));
    }
}
```

#### EventDetailViewProjector

**backend/src/main/java/com/eventmaster/query/projector/EventDetailViewProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventDetailViewProjector {
    
    private final EventDetailViewRepository repository;
    private final UserRepository userRepository;  // Denormalizacja!
    private final TicketingClient ticketingClient;  // Zapytanie do innego kontekstu
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-detail-view-projectors"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting to EventDetailView: {}", event.eventId());
        
        // Pobierz dodatkowe dane
        User organizer = userRepository.findById(event.createdBy()).orElse(null);
        
        EventDetailView view = new EventDetailView();
        view.setId(event.eventId());
        view.setName(event.name());
        view.setFullDescription(event.description());
        view.setLocationFull(event.location());
        view.setStartDate(event.startDate());
        view.setEndDate(event.startDate().plusHours(8));  // Default
        
        // Denormalizacja - dane organizatora
        if (organizer != null) {
            view.setOrganizerName(organizer.getFullName());
            view.setOrganizerEmail(organizer.getEmail());
        }
        
        repository.save(view);
    }
    
    @KafkaListener(topics = "ticketing.lifecycle", groupId = "event-detail-view-projectors")
    @Transactional
    public void onTicketTypeAdded(TicketTypeAddedEvent event) {
        // Aktualizuj ticket types w JSON
        EventDetailView view = repository.findById(event.eventId()).orElseThrow();
        
        List<TicketTypeDTO> ticketTypes = view.getTicketTypes() != null 
            ? new ArrayList<>(view.getTicketTypes())
            : new ArrayList<>();
        
        ticketTypes.add(new TicketTypeDTO(
            event.ticketTypeId(),
            event.name(),
            event.price()
        ));
        
        view.setTicketTypes(ticketTypes);
        repository.save(view);
    }
}

@Entity
@Table(name = "event_detail_view")
@Data
public class EventDetailView {
    @Id
    private UUID id;
    private String name;
    
    @Column(columnDefinition = "TEXT")
    private String fullDescription;
    
    private String locationFull;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private String organizerName;
    private String organizerEmail;
    
    @Type(JsonBinaryType.class)  // Hibernate JSON type
    @Column(columnDefinition = "jsonb")
    private List<TicketTypeDTO> ticketTypes;
    
    private Integer totalCapacity;
    private Integer availableTickets;
    
    @Type(StringArrayType.class)  // Hibernate array type
    @Column(columnDefinition = "text[]")
    private String[] tags;
}
```

#### EventSearchViewProjector

**backend/src/main/java/com/eventmaster/query/projector/EventSearchViewProjector.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventSearchViewProjector {
    
    private final JdbcTemplate jdbcTemplate;  // Native SQL dla tsvector
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-search-view-projectors"
    )
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        log.info("Projecting to EventSearchView: {}", event.eventId());
        
        String sql = """
            INSERT INTO event_search_view (id, name, description, location, tags, search_vector)
            VALUES (?, ?, ?, ?, ?, 
                to_tsvector('english', ? || ' ' || ? || ' ' || ?))
            """;
        
        String tagsArray = "{conference,technology}";  // Example
        
        jdbcTemplate.update(sql,
            event.eventId(),
            event.name(),
            event.description(),
            event.location(),
            tagsArray,
            event.name(),
            event.description(),
            event.location()
        );
    }
}
```

### 12.4 Query Handlers dla Multiple Read Models

#### ListEventsQueryHandler (używa EventListView)

```java
@Service
@RequiredArgsConstructor
public class ListEventsQueryHandler {
    
    private final EventListViewRepository repository;
    
    public Page<EventListDTO> handle(ListEventsQuery query) {
        Pageable pageable = PageRequest.of(
            query.getPage(),
            query.getSize(),
            Sort.by("startDate").descending()
        );
        
        // Szybkie zapytanie - tylko potrzebne kolumny
        Page<EventListView> views = query.getCity() != null
            ? repository.findByCity(query.getCity(), pageable)
            : repository.findAllPublished(pageable);
        
        return views.map(this::toDTO);
    }
    
    private EventListDTO toDTO(EventListView view) {
        return EventListDTO.builder()
            .id(view.getId())
            .name(view.getName())
            .city(view.getCity())
            .country(view.getCountry())
            .formattedDate(view.getFormattedDate())
            .build();
    }
}
```

#### GetEventDetailsQueryHandler (używa EventDetailView)

```java
@Service
@RequiredArgsConstructor
public class GetEventDetailsQueryHandler {
    
    private final EventDetailViewRepository repository;
    
    public EventDetailDTO handle(GetEventDetailsQuery query) {
        EventDetailView view = repository.findById(query.getEventId())
            .orElseThrow(() -> new EntityNotFoundException(
                "Event not found: " + query.getEventId()
            ));
        
        return EventDetailDTO.builder()
            .id(view.getId())
            .name(view.getName())
            .fullDescription(view.getFullDescription())
            .location(view.getLocationFull())
            .startDate(view.getStartDate())
            .endDate(view.getEndDate())
            .organizer(new OrganizerDTO(
                view.getOrganizerName(),
                view.getOrganizerEmail()
            ))
            .ticketTypes(view.getTicketTypes())
            .availableTickets(view.getAvailableTickets())
            .tags(Arrays.asList(view.getTags()))
            .build();
    }
}
```

#### SearchEventsQueryHandler (używa EventSearchView)

```java
@Service
@RequiredArgsConstructor
public class SearchEventsQueryHandler {
    
    private final JdbcTemplate jdbcTemplate;
    
    public List<EventSearchResultDTO> handle(SearchEventsQuery query) {
        // PostgreSQL Full-Text Search
        String sql = """
            SELECT id, name, description, location
            FROM event_search_view
            WHERE search_vector @@ to_tsquery('english', ?)
            ORDER BY ts_rank(search_vector, to_tsquery('english', ?)) DESC
            LIMIT ?
            """;
        
        String searchQuery = query.getQuery().replace(" ", " & ");  // AND operator
        
        return jdbcTemplate.query(sql,
            new Object[]{searchQuery, searchQuery, query.getLimit()},
            (rs, rowNum) -> new EventSearchResultDTO(
                (UUID) rs.getObject("id"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("location")
            )
        );
    }
}
```

### 12.5 Testowanie Multiple Read Models

```java
@SpringBootTest
@Testcontainers
class MultipleReadModelsTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @Autowired
    private KafkaTemplate<String, EventCreatedEvent> eventPublisher;
    
    @Autowired
    private EventListViewRepository listViewRepo;
    
    @Autowired
    private EventDetailViewRepository detailViewRepo;
    
    @Autowired
    private EventSearchViewRepository searchViewRepo;
    
    @Test
    void shouldProjectToAllThreeReadModels() {
        // Given
        EventCreatedEvent event = new EventCreatedEvent(
            UUID.randomUUID(),
            "Multi-Model Test",
            "This event will appear in all three read models",
            "Amsterdam, Netherlands",
            LocalDateTime.of(2025, 8, 10, 9, 0),
            Instant.now()
        );
        
        // When
        eventPublisher.send("events.lifecycle", event.eventId().toString(), event);
        
        // Then - wszystkie 3 read models zaktualizowane
        await().atMost(15, SECONDS).untilAsserted(() -> {
            // 1. EventListView
            Optional<EventListView> listView = listViewRepo.findById(event.eventId());
            assertThat(listView).isPresent();
            assertThat(listView.get().getCity()).isEqualTo("Amsterdam");
            
            // 2. EventDetailView
            Optional<EventDetailView> detailView = detailViewRepo.findById(event.eventId());
            assertThat(detailView).isPresent();
            assertThat(detailView.get().getFullDescription()).isNotNull();
            
            // 3. EventSearchView
            Optional<EventSearchView> searchView = searchViewRepo.findById(event.eventId());
            assertThat(searchView).isPresent();
            assertThat(searchView.get().getSearchVector()).isNotNull();
        });
    }
    
    @Test
    void eachReadModelShouldBeOptimizedForItsUseCase() {
        // Given - event w systemie
        UUID eventId = createAndPublishEvent();
        
        // When / Then - różne queries używają różnych models
        
        // 1. Lista - szybka (EventListView)
        long listStart = System.currentTimeMillis();
        List<EventListDTO> list = listEventsHandler.handle(new ListEventsQuery(0, 20));
        long listDuration = System.currentTimeMillis() - listStart;
        assertThat(listDuration).isLessThan(50);  // <50ms
        
        // 2. Szczegóły - kompletne (EventDetailView)
        EventDetailDTO details = detailsHandler.handle(new GetEventDetailsQuery(eventId));
        assertThat(details.getFullDescription()).isNotNull();
        assertThat(details.getOrganizer()).isNotNull();
        assertThat(details.getTicketTypes()).isNotNull();
        
        // 3. Wyszukiwanie - full-text (EventSearchView)
        List<EventSearchResultDTO> results = 
            searchHandler.handle(new SearchEventsQuery("test", 10));
        assertThat(results).isNotEmpty();
    }
}
```

### 12.6 Podsumowanie Tematu 12

**Kluczowe Pojęcia:**
- Multiple Read Models = różne widoki dla różnych use cases
- Każdy Read Model zoptymalizowany pod konkretne zapytania
- Denormalizacja w każdym modelu może być inna
- PostgreSQL features: JSONB, arrays, full-text search

**Best Practices:**
- ✅ Twórz Read Model per use case
- ✅ Nie kopiuj wszystkich danych do każdego modelu
- ✅ Używaj PostgreSQL features (JSONB, tsvector)
- ✅ Monitoruj wydajność każdego modelu
- ✅ Rozważ osobne bazy dla różnych Read Models (polyglot persistence)

**Następny temat:** [Rebuilding Projections](#temat-13-rebuilding-projections)

---

## Temat 13: Rebuilding Projections - Odbudowa Read Model

### 13.1 Definicja

**Rebuilding Projections** to proces odtworzenia Read Model od zera z Event Stream.

### 13.2 Kiedy Potrzebujemy Rebuilding?

1. **Zmiana logiki projekcji** - nowe pola w Read Model
2. **Błąd w projectorze** - niepoprawna logika
3. **Uszkodzenie danych** - korupcja Read Model
4. **Nowy Read Model** - dodanie zupełnie nowego widoku
5. **Migracja bazy danych** - zmiana storage

### 13.3 Strategie Rebuilding

#### Strategia 1: Full Rebuild (Pełna Odbudowa)

```
1. Stop projector
2. Truncate Read Model table
3. Replay ALL events from Event Store
4. Start projector
```

#### Strategia 2: Incremental Rebuild (Przyrostowa)

```
1. Create new Read Model table (v2)
2. Start new projector (pointing to v2)
3. Replay events to v2
4. Switch traffic: v1 → v2
5. Drop v1
```

#### Strategia 3: Snapshot + Incremental

```
1. Restore from snapshot
2. Replay events after snapshot timestamp
```

### 13.4 Implementacja - Projection Rebuilder

**backend/src/main/java/com/eventmaster/query/rebuild/ProjectionRebuilder.java**
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class ProjectionRebuilder {
    
    private final EventStoreRepository eventStore;
    private final EventViewRepository eventViewRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final EventViewProjector projector;
    
    /**
     * Pełna odbudowa EventView z Event Store
     */
    public RebuildResult rebuildEventView() {
        log.info("Starting EventView rebuild");
        Instant startTime = Instant.now();
        
        // 1. Clear existing data
        log.info("Clearing existing EventView data");
        eventViewRepository.deleteAll();
        processedEventRepository.deleteAllByEventType("EventView");
        
        // 2. Get all events from Event Store
        log.info("Fetching events from Event Store");
        List<StoredEvent> events = eventStore.findAllByAggregateType("Event")
            .stream()
            .sorted(Comparator.comparing(StoredEvent::getSequenceNumber))
            .collect(Collectors.toList());
        
        log.info("Found {} events to replay", events.size());
        
        // 3. Replay events
        int processed = 0;
        int failed = 0;
        
        for (StoredEvent storedEvent : events) {
            try {
                DomainEvent domainEvent = deserializeEvent(storedEvent);
                
                // Dispatch to projector
                if (domainEvent instanceof EventCreatedEvent) {
                    projector.onEventCreated((EventCreatedEvent) domainEvent);
                } else if (domainEvent instanceof EventPublishedEvent) {
                    projector.onEventPublished((EventPublishedEvent) domainEvent);
                } else if (domainEvent instanceof EventCancelledEvent) {
                    projector.onEventCancelled((EventCancelledEvent) domainEvent);
                }
                
                processed++;
                
                if (processed % 100 == 0) {
                    log.info("Progress: {}/{} events processed", processed, events.size());
                }
                
            } catch (Exception e) {
                log.error("Failed to process event: {}", storedEvent.getId(), e);
                failed++;
            }
        }
        
        Instant endTime = Instant.now();
        Duration duration = Duration.between(startTime, endTime);
        
        log.info("EventView rebuild completed: {} processed, {} failed, duration: {}s",
            processed, failed, duration.getSeconds());
        
        return new RebuildResult(processed, failed, duration);
    }
    
    /**
     * Odbudowa dla konkretnego eventu
     */
    public void rebuildEventView(UUID eventId) {
        log.info("Rebuilding EventView for event: {}", eventId);
        
        // Delete existing
        eventViewRepository.deleteById(eventId);
        processedEventRepository.deleteByEventIdAndEventType(eventId, "EventView");
        
        // Get events for this aggregate
        List<StoredEvent> events = eventStore.findByAggregateId(eventId)
            .stream()
            .sorted(Comparator.comparing(StoredEvent::getSequenceNumber))
            .collect(Collectors.toList());
        
        // Replay
        for (StoredEvent storedEvent : events) {
            DomainEvent domainEvent = deserializeEvent(storedEvent);
            
            if (domainEvent instanceof EventCreatedEvent) {
                projector.onEventCreated((EventCreatedEvent) domainEvent);
            } else if (domainEvent instanceof EventPublishedEvent) {
                projector.onEventPublished((EventPublishedEvent) domainEvent);
            } else if (domainEvent instanceof EventCancelledEvent) {
                projector.onEventCancelled((EventCancelledEvent) domainEvent);
            }
        }
        
        log.info("EventView rebuilt for event: {}", eventId);
    }
    
    private DomainEvent deserializeEvent(StoredEvent storedEvent) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            
            Class<?> eventClass = Class.forName(storedEvent.getEventType());
            return (DomainEvent) mapper.readValue(
                storedEvent.getPayload(), 
                eventClass
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize event", e);
        }
    }
}

@Data
@AllArgsConstructor
public class RebuildResult {
    private int processed;
    private int failed;
    private Duration duration;
}
```

### 13.5 Event Store Implementation

**backend/src/main/java/com/eventmaster/eventstore/StoredEvent.java**
```java
@Entity
@Table(name = "event_store")
@Data
@NoArgsConstructor
public class StoredEvent {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private UUID aggregateId;
    
    @Column(nullable = false)
    private String aggregateType;  // "Event", "Booking", etc.
    
    @Column(nullable = false)
    private Long sequenceNumber;  // Version/sequence in aggregate
    
    @Column(nullable = false)
    private String eventType;  // "EventCreatedEvent", etc.
    
    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;  // JSON
    
    @Column(nullable = false)
    private Instant occurredAt;
    
    @Column(nullable = false)
    private Instant storedAt;
}
```

**backend/src/main/resources/db/migration/V10__create_event_store.sql**
```sql
CREATE TABLE event_store (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    sequence_number BIGINT NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    payload TEXT NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    stored_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint: one sequence per aggregate
    UNIQUE(aggregate_id, sequence_number)
);

CREATE INDEX idx_event_store_aggregate ON event_store(aggregate_id, sequence_number);
CREATE INDEX idx_event_store_type ON event_store(aggregate_type, stored_at);
CREATE INDEX idx_event_store_occurred ON event_store(occurred_at);
```

### 13.6 REST API dla Rebuilding

**backend/src/main/java/com/eventmaster/admin/ProjectionAdminController.java**
```java
@RestController
@RequestMapping("/api/admin/projections")
@RequiredArgsConstructor
@Slf4j
public class ProjectionAdminController {
    
    private final ProjectionRebuilder rebuilder;
    
    @PostMapping("/rebuild/event-view")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<RebuildResult> rebuildEventView() {
        log.info("Rebuild EventView requested");
        
        RebuildResult result = rebuilder.rebuildEventView();
        
        return ResponseEntity.ok(result);
    }
    
    @PostMapping("/rebuild/event-view/{eventId}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<Void> rebuildEventView(@PathVariable UUID eventId) {
        log.info("Rebuild EventView requested for event: {}", eventId);
        
        rebuilder.rebuildEventView(eventId);
        
        return ResponseEntity.ok().build();
    }
    
    @GetMapping("/status")
    public ResponseEntity<ProjectionStatus> getStatus() {
        // Return stats: total events, processed, pending, etc.
        return ResponseEntity.ok(new ProjectionStatus(/* ... */));
    }
}
```

### 13.7 Testowanie Rebuilding

```java
@SpringBootTest
@Testcontainers
class ProjectionRebuildingTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Autowired
    private ProjectionRebuilder rebuilder;
    
    @Autowired
    private EventStoreRepository eventStore;
    
    @Autowired
    private EventViewRepository eventViewRepository;
    
    @Test
    void shouldRebuildEventViewFromEventStore() {
        // Given - 100 events w Event Store
        List<UUID> eventIds = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            UUID eventId = UUID.randomUUID();
            eventIds.add(eventId);
            
            // EventCreatedEvent
            StoredEvent created = new StoredEvent();
            created.setAggregateId(eventId);
            created.setAggregateType("Event");
            created.setSequenceNumber(1L);
            created.setEventType("EventCreatedEvent");
            created.setPayload(createEventCreatedJson(eventId, "Event " + i));
            created.setOccurredAt(Instant.now());
            eventStore.save(created);
            
            // EventPublishedEvent (co drugi)
            if (i % 2 == 0) {
                StoredEvent published = new StoredEvent();
                published.setAggregateId(eventId);
                published.setAggregateType("Event");
                published.setSequenceNumber(2L);
                published.setEventType("EventPublishedEvent");
                published.setPayload(createEventPublishedJson(eventId));
                published.setOccurredAt(Instant.now());
                eventStore.save(published);
            }
        }
        
        // When - rebuild
        RebuildResult result = rebuilder.rebuildEventView();
        
        // Then - wszystkie eventy przetworzone
        assertThat(result.getProcessed()).isEqualTo(150);  // 100 created + 50 published
        assertThat(result.getFailed()).isEqualTo(0);
        
        // EventView zawiera 100 eventów
        List<EventView> views = eventViewRepository.findAll();
        assertThat(views).hasSize(100);
        
        // 50 jest published
        long publishedCount = views.stream()
            .filter(EventView::getIsPublished)
            .count();
        assertThat(publishedCount).isEqualTo(50);
    }
    
    @Test
    void shouldRebuildSingleEventView() {
        // Given - event w Event Store
        UUID eventId = UUID.randomUUID();
        
        StoredEvent created = createStoredEvent(eventId, 1L, "EventCreatedEvent");
        eventStore.save(created);
        
        StoredEvent published = createStoredEvent(eventId, 2L, "EventPublishedEvent");
        eventStore.save(published);
        
        // When - rebuild pojedynczego eventu
        rebuilder.rebuildEventView(eventId);
        
        // Then
        EventView view = eventViewRepository.findById(eventId).orElseThrow();
        assertThat(view.getStatus()).isEqualTo("PUBLISHED");
        assertThat(view.getIsPublished()).isTrue();
    }
    
    @Test
    void shouldHandleCorruptedReadModel() {
        // Given - uszkodzone dane w Read Model
        EventView corruptedView = new EventView();
        corruptedView.setId(UUID.randomUUID());
        corruptedView.setName("Corrupted");
        corruptedView.setStatus("INVALID_STATUS");
        eventViewRepository.save(corruptedView);
        
        // Event Store ma poprawne dane
        StoredEvent event = createStoredEvent(corruptedView.getId(), 1L, "EventCreatedEvent");
        eventStore.save(event);
        
        // When - rebuild
        rebuilder.rebuildEventView(corruptedView.getId());
        
        // Then - dane naprawione
        EventView fixed = eventViewRepository.findById(corruptedView.getId()).orElseThrow();
        assertThat(fixed.getStatus()).isEqualTo("DRAFT");
    }
}
```

### 13.8 Monitoring Rebuilding Process

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class MonitoredProjectionRebuilder {
    
    private final ProjectionRebuilder rebuilder;
    private final MeterRegistry meterRegistry;
    
    public RebuildResult rebuildEventView() {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            RebuildResult result = rebuilder.rebuildEventView();
            
            // Metrics
            meterRegistry.gauge("projection.rebuild.processed", result.getProcessed());
            meterRegistry.gauge("projection.rebuild.failed", result.getFailed());
            
            sample.stop(meterRegistry.timer("projection.rebuild.duration",
                "projection", "EventView",
                "status", "success"
            ));
            
            return result;
            
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("projection.rebuild.duration",
                "projection", "EventView",
                "status", "failure"
            ));
            
            throw e;
        }
    }
}
```

### 13.9 Podsumowanie Tematu 13

**Kluczowe Pojęcia:**
- Rebuilding = odtworzenie Read Model z Event Store
- Event Store = append-only log wszystkich events
- Full rebuild vs Incremental rebuild
- Idempotentność jest krytyczna

**Best Practices:**
- ✅ Zawsze zachowuj Event Store (nigdy nie usuwaj!)
- ✅ Testuj rebuilding regularnie
- ✅ Monitoruj czas rebuilding
- ✅ Używaj batch processing dla wydajności
- ✅ Rozważ Blue-Green deployment dla Read Models

---

## Temat 14: Optimistic Concurrency Control - Wersjonowanie

### 14.1 Definicja

**Optimistic Locking** zakłada, że konflikty są rzadkie. Używamy **version field** do wykrywania konfliktów współbieżnych.

### 14.2 Problem: Concurrent Updates

```
Time    User A                      User B
─────────────────────────────────────────────────
t0      Read Event (version=1)      Read Event (version=1)
t1      Modify name                 Modify description
t2      Save (version=1→2) ✅
t3                                   Save (version=1→2) ❌ CONFLICT!
```

Bez optimistic locking: **Lost Update Problem** - zmiany User A zostałyby nadpisane!

### 14.3 Implementacja - JPA @Version

**backend/src/main/java/com/eventmaster/domain/Event.java**
```java
@Entity
@Table(name = "events")
@Data
@NoArgsConstructor
public class Event {
    
    @Id
    private UUID id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(columnDefinition = "TEXT")
    private String description;
    
    @Version  // ← Optimistic Locking!
    @Column(nullable = false)
    private Long version;
    
    @Column(name = "updated_at")
    private Instant updatedAt;
    
    public void updateName(String newName) {
        this.name = newName;
        this.updatedAt = Instant.now();
        // version will be incremented automatically by JPA
    }
}
```

**Jak to działa:**

```sql
-- JPA generates:
UPDATE events 
SET name = ?, 
    description = ?, 
    updated_at = ?,
    version = version + 1  -- Increment!
WHERE id = ? 
  AND version = ?;  -- Check current version!

-- If version doesn't match → 0 rows updated → OptimisticLockException
```

### 14.4 Testowanie Optimistic Locking

```java
@SpringBootTest
@Testcontainers
class OptimisticLockingTest {
    
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");
    
    @Autowired
    private EventRepository eventRepository;
    
    @Autowired
    private TestEntityManager entityManager;
    
    @Test
    void shouldDetectConcurrentModification() {
        // Given - event w bazie
        Event event = Event.create(
            UUID.randomUUID(),
            "Original Name",
            "Description",
            "Location",
            LocalDateTime.now().plusDays(1)
        );
        eventRepository.save(event);
        UUID eventId = event.getId();
        
        // When - dwa użytkownicy czytają ten sam event
        Event userA = eventRepository.findById(eventId).orElseThrow();
        Event userB = eventRepository.findById(eventId).orElseThrow();
        
        // User A modyfikuje i zapisuje
        userA.updateName("Name by User A");
        eventRepository.saveAndFlush(userA);  // version: 0 → 1
        
        // User B próbuje zapisać (ma starą wersję!)
        userB.updateName("Name by User B");
        
        // Then - OptimisticLockException!
        assertThatThrownBy(() -> eventRepository.saveAndFlush(userB))
            .isInstanceOf(OptimisticLockException.class);
        
        // User A's change persisted
        Event persisted = eventRepository.findById(eventId).orElseThrow();
        assertThat(persisted.getName()).isEqualTo("Name by User A");
        assertThat(persisted.getVersion()).isEqualTo(1L);
    }
    
    @Test
    void shouldAllowSequentialUpdates() {
        // Given
        Event event = createAndSaveEvent();
        
        // When - sekwencyjne zmiany (bez konfliktu)
        event.updateName("Name 1");
        eventRepository.saveAndFlush(event);  // version: 0 → 1
        
        event.updateDescription("Description 1");
        eventRepository.saveAndFlush(event);  // version: 1 → 2
        
        event.updateName("Name 2");
        eventRepository.saveAndFlush(event);  // version: 2 → 3
        
        // Then
        Event persisted = eventRepository.findById(event.getId()).orElseThrow();
        assertThat(persisted.getVersion()).isEqualTo(3L);
        assertThat(persisted.getName()).isEqualTo("Name 2");
    }
}
```

### 14.5 Obsługa OptimisticLockException

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventCommandService {
    
    private final EventRepository eventRepository;
    
    @Transactional
    public void updateEvent(UUID eventId, UpdateEventCommand command) {
        int maxRetries = 3;
        int attempt = 0;
        
        while (attempt < maxRetries) {
            try {
                Event event = eventRepository.findById(eventId)
                    .orElseThrow(() -> new EntityNotFoundException("Event not found"));
                
                // Apply changes
                event.updateName(command.getName());
                event.updateDescription(command.getDescription());
                
                // Save (might throw OptimisticLockException)
                eventRepository.saveAndFlush(event);
                
                log.info("Event updated successfully: {}", eventId);
                return;
                
            } catch (OptimisticLockException e) {
                attempt++;
                log.warn("Optimistic lock conflict, attempt {}/{}", attempt, maxRetries);
                
                if (attempt >= maxRetries) {
                    throw new ConcurrencyException(
                        "Failed to update event after " + maxRetries + " attempts", e
                    );
                }
                
                // Clear persistence context and retry
                entityManager.clear();
                
                // Exponential backoff
                try {
                    Thread.sleep(100 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }
}
```

### 14.6 REST API z Version Handling

```java
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventCommandController {
    
    @PutMapping("/{id}")
    public ResponseEntity<?> updateEvent(
        @PathVariable UUID id,
        @RequestBody UpdateEventDTO dto,
        @RequestHeader(value = "If-Match", required = false) Long expectedVersion
    ) {
        try {
            // Check version if provided
            if (expectedVersion != null) {
                Event current = eventRepository.findById(id).orElseThrow();
                if (!current.getVersion().equals(expectedVersion)) {
                    return ResponseEntity
                        .status(HttpStatus.PRECONDITION_FAILED)
                        .body(new ErrorResponse(
                            "Version conflict",
                            "Expected version: " + expectedVersion + 
                            ", current: " + current.getVersion()
                        ));
                }
            }
            
            eventService.updateEvent(id, dto.toCommand());
            
            Event updated = eventRepository.findById(id).orElseThrow();
            
            return ResponseEntity.ok()
                .header("ETag", updated.getVersion().toString())
                .body(toDTO(updated));
                
        } catch (OptimisticLockException e) {
            return ResponseEntity
                .status(HttpStatus.CONFLICT)
                .body(new ErrorResponse(
                    "Concurrent modification detected",
                    "Please refresh and try again"
                ));
        }
    }
}
```

### 14.7 Frontend Handling (Nuxt 3)

```typescript
// composables/useEventUpdate.ts
export const useEventUpdate = () => {
  const updateEvent = async (
    eventId: string, 
    data: UpdateEventData, 
    currentVersion: number
  ) => {
    try {
      const response = await $fetch(`/api/v1/events/${eventId}`, {
        method: 'PUT',
        headers: {
          'If-Match': currentVersion.toString()
        },
        body: data
      })
      
      return {
        success: true,
        data: response,
        newVersion: parseInt(response.headers.get('ETag'))
      }
      
    } catch (error: any) {
      if (error.status === 409 || error.status === 412) {
        // Conflict - show merge dialog
        return {
          success: false,
          error: 'CONFLICT',
          message: 'Event was modified by another user'
        }
      }
      
      throw error
    }
  }
  
  return { updateEvent }
}
```

### 14.8 Podsumowanie Tematu 14

**Kluczowe Pojęcia:**
- Optimistic Locking = wykrywanie konfliktów przez version
- @Version = automatyczny increment przez JPA
- OptimisticLockException = konflikt wykryty
- Retry z exponential backoff

**Best Practices:**
- ✅ Zawsze używaj @Version w aggregates
- ✅ Obsługuj OptimisticLockException (retry lub inform user)
- ✅ Używaj ETag w HTTP headers
- ✅ Frontend powinien pokazać merge conflict UI
- ✅ Log wszystkie konflikty dla analizy

---

## Temat 15: Command Validation - Walidacja Komend

### 15.1 Poziomy Walidacji

```
┌─────────────────────────────────────┐
│  1. SYNTAX VALIDATION                │
│  (DTO layer - annotations)           │
│  - @NotNull, @Size, @Email          │
└─────────────────┬───────────────────┘
                  ↓
┌─────────────────────────────────────┐
│  2. SEMANTIC VALIDATION              │
│  (Command layer - business rules)    │
│  - Date in future                    │
│  - Valid location format             │
└─────────────────┬───────────────────┘
                  ↓
┌─────────────────────────────────────┐
│  3. BUSINESS VALIDATION              │
│  (Handler - database checks)         │
│  - Event name unique                 │
│  - User has permissions              │
└─────────────────────────────────────┘
```

### 15.2 Level 1: Syntax Validation (DTO)

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateEventDTO {
    
    @NotBlank(message = "Event name is required")
    @Size(min = 3, max = 200, message = "Name must be between 3 and 200 characters")
    private String name;
    
    @NotBlank(message = "Description is required")
    @Size(min = 10, max = 5000, message = "Description must be between 10 and 5000 characters")
    private String description;
    
    @NotBlank(message = "Location is required")
    @Pattern(regexp = "^[A-Za-z\\s]+,\\s*[A-Za-z\\s]+$", 
             message = "Location must be in format: City, Country")
    private String location;
    
    @NotNull(message = "Start date is required")
    @Future(message = "Start date must be in the future")
    private LocalDateTime startDate;
    
    @Min(value = 10, message = "Capacity must be at least 10")
    @Max(value = 100000, message = "Capacity cannot exceed 100,000")
    private Integer capacity;
    
    @Email(message = "Invalid email format")
    private String contactEmail;
}

@RestController
@RequestMapping("/api/v1/events")
public class EventCommandController {
    
    @PostMapping
    public ResponseEntity<?> createEvent(@Valid @RequestBody CreateEventDTO dto) {
        // @Valid triggers validation
        // If validation fails → 400 Bad Request automatically
    }
}
```

### 15.3 Level 2: Semantic Validation (Command)

```java
public record CreateEventCommand(
    UUID eventId,
    String name,
    String description,
    String location,
    LocalDateTime startDate,
    Integer capacity
) {
    public void validate() {
        List<String> errors = new ArrayList<>();
        
        // Business-specific rules
        if (startDate.isBefore(LocalDateTime.now())) {
            errors.add("Event cannot start in the past");
        }
        
        if (startDate.isAfter(LocalDateTime.now().plusYears(2))) {
            errors.add("Event cannot be scheduled more than 2 years in advance");
        }
        
        if (capacity != null && capacity % 10 != 0) {
            errors.add("Capacity must be multiple of 10");
        }
        
        // Location validation
        String[] locationParts = location.split(",");
        if (locationParts.length != 2) {
            errors.add("Location must contain city and country");
        }
        
        if (!errors.isEmpty()) {
            throw new CommandValidationException(errors);
        }
    }
}
```

### 15.4 Level 3: Business Validation (Handler)

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class CreateEventCommandHandler {
    
    private final EventRepository eventRepository;
    private final UserRepository userRepository;
    
    @KafkaListener(topics = "commands.events.create")
    @Transactional
    public void handle(CreateEventCommand command) {
        log.info("Handling CreateEventCommand: {}", command.eventId());
        
        // Level 3: Business validation (database checks)
        validateBusinessRules(command);
        
        // Create event
        Event event = Event.create(/* ... */);
        eventRepository.save(event);
        
        // Publish event
        // ...
    }
    
    private void validateBusinessRules(CreateEventCommand command) {
        List<String> errors = new ArrayList<>();
        
        // 1. Check if event name is unique
        if (eventRepository.existsByName(command.name())) {
            errors.add("Event with this name already exists");
        }
        
        // 2. Check if location exists in our database
        if (!locationService.isValidLocation(command.location())) {
            errors.add("Invalid location: " + command.location());
        }
        
        // 3. Check if user has permission
        User creator = userRepository.findById(command.createdBy())
            .orElseThrow(() -> new EntityNotFoundException("User not found"));
        
        if (!creator.hasRole("EVENT_ORGANIZER")) {
            errors.add("User does not have permission to create events");
        }
        
        // 4. Check if capacity is available at venue
        if (!venueService.hasCapacity(command.location(), command.capacity())) {
            errors.add("Venue does not have requested capacity");
        }
        
        if (!errors.isEmpty()) {
            throw new BusinessValidationException(errors);
        }
    }
}
```

### 15.5 Custom Validators

```java
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = FutureDateValidator.class)
public @interface FutureDate {
    String message() default "Date must be in the future";
    int minDays() default 1;
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}

public class FutureDateValidator implements ConstraintValidator<FutureDate, LocalDateTime> {
    
    private int minDays;
    
    @Override
    public void initialize(FutureDate annotation) {
        this.minDays = annotation.minDays();
    }
    
    @Override
    public boolean isValid(LocalDateTime value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;  // @NotNull handles null
        }
        
        LocalDateTime minDate = LocalDateTime.now().plusDays(minDays);
        return value.isAfter(minDate);
    }
}

// Usage:
@Data
public class CreateEventDTO {
    @FutureDate(minDays = 7, message = "Event must be at least 7 days in the future")
    private LocalDateTime startDate;
}
```

### 15.6 Testowanie Walidacji

```java
@SpringBootTest
class CommandValidationTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void shouldRejectInvalidSyntax() throws Exception {
        // Given - invalid DTO
        String invalidJson = """
            {
                "name": "AB",
                "description": "Too short",
                "location": "InvalidFormat",
                "startDate": "2020-01-01T10:00:00"
            }
            """;
        
        // When / Then
        mockMvc.perform(post("/api/v1/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors").isArray())
            .andExpect(jsonPath("$.errors[*].field").value(hasItems("name", "description", "location", "startDate")));
    }
    
    @Test
    void shouldRejectSemanticErrors() {
        // Given
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            "Valid Name",
            "Valid Description",
            "Berlin, Germany",
            LocalDateTime.now().plusYears(3),  // Too far in future!
            25  // Not multiple of 10!
        );
        
        // When / Then
        assertThatThrownBy(() -> command.validate())
            .isInstanceOf(CommandValidationException.class)
            .hasMessageContaining("more than 2 years")
            .hasMessageContaining("multiple of 10");
    }
    
    @Test
    void shouldRejectBusinessRuleViolations() {
        // Given - event już istnieje
        Event existing = createAndSaveEvent("Duplicate Name");
        
        CreateEventCommand command = new CreateEventCommand(
            UUID.randomUUID(),
            "Duplicate Name",  // Duplikat!
            "Description",
            "Location",
            LocalDateTime.now().plusDays(30),
            100
        );
        
        // When / Then
        assertThatThrownBy(() -> handler.handle(command))
            .isInstanceOf(BusinessValidationException.class)
            .hasMessageContaining("already exists");
    }
}
```

### 15.7 Podsumowanie Tematu 15

**Kluczowe Pojęcia:**
- 3 poziomy walidacji: Syntax, Semantic, Business
- Bean Validation (@Valid, @NotNull, etc.)
- Custom validators
- Early validation = less wasted processing

**Best Practices:**
- ✅ Waliduj jak najwcześniej (fail fast)
- ✅ Zwracaj wszystkie błędy naraz (nie tylko pierwszy)
- ✅ Używaj jasnych komunikatów
- ✅ Testuj każdy poziom osobno

---

## Temat 16: Command Handler Pattern

### 16.1 Definicja

**Command Handler Pattern** = jeden handler na jeden typ komendy, odpowiedzialny za wykonanie logiki biznesowej.

### 16.2 Struktura Command Handler

```java
public interface CommandHandler<C extends Command> {
    void handle(C command);
}

@Service
public class CreateEventCommandHandler implements CommandHandler<CreateEventCommand> {
    
    @Override
    @Transactional
    public void handle(CreateEventCommand command) {
        // 1. Validate
        // 2. Execute business logic
        // 3. Publish events
    }
}
```

### 16.3 Implementacja - Command Bus

```java
@Service
@RequiredArgsConstructor
public class CommandBus {
    
    private final ApplicationContext context;
    private final Map<Class<?>, CommandHandler<?>> handlers = new ConcurrentHashMap<>();
    
    @SuppressWarnings("unchecked")
    public <C extends Command> void dispatch(C command) {
        CommandHandler<C> handler = (CommandHandler<C>) handlers.computeIfAbsent(
            command.getClass(),
            this::findHandler
        );
        
        handler.handle(command);
    }
    
    private CommandHandler<?> findHandler(Class<?> commandClass) {
        return context.getBeansOfType(CommandHandler.class)
            .values()
            .stream()
            .filter(h -> supportsCommand(h, commandClass))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No handler found for command: " + commandClass.getName()
            ));
    }
}
```

### 16.4 Testowanie

```java
@SpringBootTest
class CommandHandlerPatternTest {
    
    @Autowired
    private CommandBus commandBus;
    
    @Autowired
    private EventRepository eventRepository;
    
    @Test
    void shouldDispatchToCorrectHandler() {
        // Given
        CreateEventCommand command = new CreateEventCommand(/* ... */);
        
        // When
        commandBus.dispatch(command);
        
        // Then
        assertThat(eventRepository.findAll()).hasSize(1);
    }
}
```

---

## Temat 17: Query Handler Pattern

### 17.1 Implementacja

```java
public interface QueryHandler<Q extends Query, R> {
    R handle(Q query);
}

@Service
@RequiredArgsConstructor
public class ListEventsQueryHandler implements QueryHandler<ListEventsQuery, Page<EventListDTO>> {
    
    private final EventListViewRepository repository;
    
    @Override
    public Page<EventListDTO> handle(ListEventsQuery query) {
        Pageable pageable = PageRequest.of(query.getPage(), query.getSize());
        
        Page<EventListView> views = repository.findAllPublished(pageable);
        
        return views.map(this::toDTO);
    }
}
```

### 17.2 Query Bus

```java
@Service
@RequiredArgsConstructor
public class QueryBus {
    
    private final ApplicationContext context;
    
    @SuppressWarnings("unchecked")
    public <Q extends Query, R> R execute(Q query) {
        QueryHandler<Q, R> handler = findHandler(query.getClass());
        return handler.handle(query);
    }
}
```

---

## Temat 18: Denormalization - Duplikacja Danych

### 18.1 Przykład Denormalizacji

**Normalized (Write Model):**
```sql
Table: events
- id
- name
- organizer_id (FK)

Table: users
- id
- first_name
- last_name
```

**Denormalized (Read Model):**
```sql
Table: event_view
- id
- name
- organizer_full_name  -- "John Doe" (denormalized!)
- organizer_email      -- "john@example.com" (denormalized!)
```

### 18.2 Implementacja

```java
@Service
@RequiredArgsConstructor
public class EventViewProjector {
    
    private final EventViewRepository eventViewRepository;
    private final UserRepository userRepository;
    
    @KafkaListener(topics = "events.lifecycle")
    @Transactional
    public void onEventCreated(EventCreatedEvent event) {
        // Fetch related data
        User organizer = userRepository.findById(event.createdBy())
            .orElse(null);
        
        EventView view = new EventView();
        view.setId(event.eventId());
        view.setName(event.name());
        
        // DENORMALIZATION - copy user data into event view
        if (organizer != null) {
            view.setOrganizerFullName(
                organizer.getFirstName() + " " + organizer.getLastName()
            );
            view.setOrganizerEmail(organizer.getEmail());
        }
        
        eventViewRepository.save(view);
    }
    
    // Update when user changes name
    @KafkaListener(topics = "users.lifecycle")
    @Transactional
    public void onUserNameChanged(UserNameChangedEvent event) {
        // Find all events by this organizer
        List<EventView> views = eventViewRepository.findByOrganizerId(event.userId());
        
        // Update denormalized data
        String newFullName = event.firstName() + " " + event.lastName();
        views.forEach(v -> v.setOrganizerFullName(newFullName));
        
        eventViewRepository.saveAll(views);
    }
}
```

### 18.3 Trade-offs

**Zalety:**
- ⚡ Szybkie odczyty (no JOINs)
- 📊 Lepsze cache hit ratio
- 🚀 Skalowalność

**Wady:**
- 💾 Więcej przestrzeni dyskowej
- 🔄 Eventual consistency
- 🔧 Więcej kodu do utrzymania

---

## Temat 19: Materialized Views - PostgreSQL

### 19.1 Definicja

**Materialized View** to prekalkulowany wynik zapytania zapisany jako fizyczna tabela.

### 19.2 Implementacja w PostgreSQL

```sql
-- Create materialized view
CREATE MATERIALIZED VIEW event_statistics_mv AS
SELECT 
    e.id,
    e.name,
    COUNT(DISTINCT b.id) as booking_count,
    COUNT(DISTINCT t.id) as ticket_count,
    SUM(b.total_price) as total_revenue,
    AVG(r.rating) as avg_rating
FROM events e
LEFT JOIN bookings b ON e.id = b.event_id
LEFT JOIN tickets t ON e.id = t.event_id
LEFT JOIN reviews r ON e.id = r.event_id
GROUP BY e.id, e.name;

-- Create index on materialized view
CREATE INDEX idx_event_stats_mv_id ON event_statistics_mv(id);
CREATE INDEX idx_event_stats_mv_revenue ON event_statistics_mv(total_revenue DESC);

-- Refresh materialized view (CONCURRENTLY for no locking)
REFRESH MATERIALIZED VIEW CONCURRENTLY event_statistics_mv;
```

### 19.3 Auto-Refresh Strategy

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class MaterializedViewRefresher {
    
    private final JdbcTemplate jdbcTemplate;
    
    @Scheduled(cron = "0 */30 * * * *")  // Every 30 minutes
    public void refreshEventStatistics() {
        log.info("Refreshing event_statistics_mv");
        
        long start = System.currentTimeMillis();
        jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY event_statistics_mv");
        long duration = System.currentTimeMillis() - start;
        
        log.info("event_statistics_mv refreshed in {}ms", duration);
    }
}
```

### 19.4 Event-Driven Refresh

```java
@Service
@RequiredArgsConstructor
public class SmartMaterializedViewRefresher {
    
    private final JdbcTemplate jdbcTemplate;
    private volatile boolean refreshNeeded = false;
    
    @KafkaListener(topics = {"bookings.lifecycle", "reviews.lifecycle"})
    public void onDataChanged(Object event) {
        refreshNeeded = true;
    }
    
    @Scheduled(fixedDelay = 60000)  // Every minute
    public void refreshIfNeeded() {
        if (refreshNeeded) {
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY event_statistics_mv");
            refreshNeeded = false;
        }
    }
}
```

---

## Temat 20: Snapshot Pattern - Zrzuty Stanu

### 20.1 Definicja

**Snapshot** to zrzut stanu aggregate w określonym momencie, używany do optymalizacji odtwarzania z Event Store.

### 20.2 Problem: Zbyt Wiele Events

```
Event Stream dla Event ID=123:
1. EventCreated
2. EventPublished
3. TicketAdded
4. TicketAdded
... (997 more events)
1000. EventCancelled

Bez snapshot: replay 1000 events! 😱
Z snapshot: load snapshot #900 + replay 100 events ✅
```

### 20.3 Implementacja - Snapshot Entity

```java
@Entity
@Table(name = "event_snapshots")
@Data
@NoArgsConstructor
public class EventSnapshot {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private UUID eventId;
    
    @Column(nullable = false)
    private Long version;  // Event version at snapshot time
    
    @Column(columnDefinition = "TEXT", nullable = false)
    private String state;  // JSON serialized Event state
    
    @Column(nullable = false)
    private Instant createdAt;
}
```

**Migration:**
```sql
CREATE TABLE event_snapshots (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    version BIGINT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    
    UNIQUE(event_id, version)
);

CREATE INDEX idx_event_snapshots_latest 
    ON event_snapshots(event_id, version DESC);
```

### 20.4 Snapshot Strategy

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EventSnapshotService {
    
    private final EventSnapshotRepository snapshotRepository;
    private final EventStoreRepository eventStore;
    private final ObjectMapper objectMapper;
    
    private static final int SNAPSHOT_INTERVAL = 100;  // Every 100 events
    
    public void createSnapshotIfNeeded(Event event) {
        if (event.getVersion() % SNAPSHOT_INTERVAL == 0) {
            createSnapshot(event);
        }
    }
    
    private void createSnapshot(Event event) {
        try {
            String stateJson = objectMapper.writeValueAsString(event);
            
            EventSnapshot snapshot = new EventSnapshot();
            snapshot.setEventId(event.getId());
            snapshot.setVersion(event.getVersion());
            snapshot.setState(stateJson);
            snapshot.setCreatedAt(Instant.now());
            
            snapshotRepository.save(snapshot);
            
            log.info("Snapshot created for event {} at version {}", 
                event.getId(), event.getVersion());
                
        } catch (Exception e) {
            log.error("Failed to create snapshot", e);
        }
    }
    
    public Event reconstruct(UUID eventId) {
        // 1. Load latest snapshot
        Optional<EventSnapshot> snapshotOpt = 
            snapshotRepository.findLatestByEventId(eventId);
        
        Event event;
        long fromVersion;
        
        if (snapshotOpt.isPresent()) {
            // Start from snapshot
            EventSnapshot snapshot = snapshotOpt.get();
            event = deserializeEvent(snapshot.getState());
            fromVersion = snapshot.getVersion() + 1;
            
            log.info("Loaded snapshot for event {} at version {}", 
                eventId, snapshot.getVersion());
        } else {
            // No snapshot, start from scratch
            event = new Event();
            fromVersion = 0;
        }
        
        // 2. Replay events after snapshot
        List<StoredEvent> events = eventStore.findByEventIdAndVersionGreaterThan(
            eventId, fromVersion
        );
        
        log.info("Replaying {} events from version {}", events.size(), fromVersion);
        
        for (StoredEvent storedEvent : events) {
            DomainEvent domainEvent = deserializeDomainEvent(storedEvent);
            event.apply(domainEvent);
        }
        
        return event;
    }
    
    private Event deserializeEvent(String json) {
        try {
            return objectMapper.readValue(json, Event.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize event", e);
        }
    }
}
```

### 20.5 Testowanie Snapshot

```java
@SpringBootTest
@Testcontainers
class EventSnapshotTest {
    
    @Autowired
    private EventSnapshotService snapshotService;
    
    @Autowired
    private EventStoreRepository eventStore;
    
    @Test
    void shouldCreateSnapshotEvery100Events() {
        // Given - event z 250 events
        UUID eventId = UUID.randomUUID();
        
        for (int i = 1; i <= 250; i++) {
            StoredEvent event = createStoredEvent(eventId, i);
            eventStore.save(event);
        }
        
        Event event = reconstructEvent(eventId);
        event.setVersion(250L);
        
        // When - trigger snapshot logic
        snapshotService.createSnapshotIfNeeded(event);
        
        // Then - snapshots at version 100, 200
        List<EventSnapshot> snapshots = 
            snapshotRepository.findAllByEventId(eventId);
        
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.get(0).getVersion()).isEqualTo(100L);
        assertThat(snapshots.get(1).getVersion()).isEqualTo(200L);
    }
    
    @Test
    void shouldReconstructFromSnapshot() {
        // Given - 150 events + snapshot at 100
        UUID eventId = UUID.randomUUID();
        
        // Events 1-150
        for (int i = 1; i <= 150; i++) {
            eventStore.save(createStoredEvent(eventId, i));
        }
        
        // Snapshot at version 100
        Event eventAt100 = reconstructUpTo(eventId, 100);
        snapshotService.createSnapshot(eventAt100);
        
        // When - reconstruct
        Event reconstructed = snapshotService.reconstruct(eventId);
        
        // Then - only replayed 50 events (101-150), not all 150!
        verify(eventStore).findByEventIdAndVersionGreaterThan(eventId, 100L);
    }
}
```

### 20.6 Cleanup Strategy

```java
@Service
@RequiredArgsConstructor
public class SnapshotCleanupService {
    
    private final EventSnapshotRepository repository;
    
    @Scheduled(cron = "0 0 2 * * *")  // 2 AM daily
    public void cleanupOldSnapshots() {
        // Keep only last 3 snapshots per event
        List<UUID> eventIds = repository.findDistinctEventIds();
        
        for (UUID eventId : eventIds) {
            List<EventSnapshot> snapshots = repository
                .findByEventIdOrderByVersionDesc(eventId);
            
            if (snapshots.size() > 3) {
                List<EventSnapshot> toDelete = snapshots.subList(3, snapshots.size());
                repository.deleteAll(toDelete);
                
                log.info("Deleted {} old snapshots for event {}", 
                    toDelete.size(), eventId);
            }
        }
    }
}
```

---

## 🎓 Podsumowanie CZĘŚĆ II

### ✅ Ukończone Tematy (11-20):

11. **Projections** - budowanie Read Model z Event Stream
12. **Multiple Read Models** - różne widoki dla różnych use cases
13. **Rebuilding Projections** - odbudowa Read Model
14. **Optimistic Concurrency Control** - wykrywanie konfliktów
15. **Command Validation** - 3 poziomy walidacji
16. **Command Handler Pattern** - jeden handler per command
17. **Query Handler Pattern** - CQRS read side
18. **Denormalization** - duplikacja danych dla wydajności
19. **Materialized Views** - PostgreSQL prekalkulowane widoki
20. **Snapshot Pattern** - optymalizacja odtwarzania z Event Store

### 🎯 Zdobyte Umiejętności:

- ✅ Projektowanie i implementacja projectorów
- ✅ Tworzenie wielu Read Models dla różnych use cases
- ✅ Odbudowa Read Models z Event Store
- ✅ Obsługa współbieżności przez Optimistic Locking
- ✅ Wielopoziomowa walidacja komend
- ✅ Wzorce Command i Query Handlers
- ✅ Denormalizacja danych w Read Models
- ✅ Wykorzystanie PostgreSQL Materialized Views
- ✅ Optymalizacja przez Snapshot Pattern

### 🛠️ Stack Technologiczny:

- **PostgreSQL 18** - Write DB, Read DB, Event Store
- **Apache Kafka 7.6** - Event streaming
- **Spring Boot 3.4** - Backend framework
- **JPA/Hibernate** - ORM z @Version
- **Bean Validation** - walidacja DTO
- **Testcontainers** - testy integracyjne
- **Flyway** - migracje bazy danych

### 📊 Metryki Dokumentacji:

```
CZĘŚĆ I:  3,788 linii (Tematy 1-10)
CZĘŚĆ II: ~3,500 linii (Tematy 11-20)
─────────────────────────────────────
RAZEM:    ~7,300 linii dokumentacji!
```

### 🚀 Następne Kroki:

Teraz masz kompletną wiedzę o:
- Message & Event-Driven Architecture (CZĘŚĆ I)
- CQRS w praktyce (CZĘŚĆ II)

**Gotowy do implementacji:**
1. Zbuduj EventMaster od podstaw
2. Zastosuj wszystkie wzorce
3. Napisz kompleksowe testy
4. Wdróż na produkcję

### 📚 Dodatkowe Materiały:

- **CZĘŚĆ III: Event Sourcing** (Tematy 21-30) - opcjonalne zaawansowane
- **CZĘŚĆ IV: Messaging & Kafka** (Tematy 31-40) - deep dive
- **CZĘŚĆ V: Testowanie & Operacje** (Tematy 41-50) - DevOps

---

**Dokument ukończony:** 2025-01-20  
**Wersja:** 1.0  
**Autorzy:** EventMaster Architecture Team  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4 + Nuxt 3  
**Status:** ✅ CZĘŚĆ I i II UKOŃCZONE

**Powodzenia w budowie EventMaster! 🎉**

