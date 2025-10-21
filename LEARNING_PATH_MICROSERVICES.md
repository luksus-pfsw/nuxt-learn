# 🎓 EventMaster - Ścieżka Nauki: Message & Event-Driven Architecture dla Testera

**Autor:** Architekt EventMaster (bazując na Vaughn Vernon - Addison-Wesley Signature Series)  
**Dla kogo:** Tester automatyczny chcący zrozumieć CQRS, Event Sourcing i systemy rozproszone  
**Cel:** Kompleksowa wiedza potrzebna do testowania i rozwijania EventMaster  
**Czas nauki:** 12-16 tygodni (50 tematów)

---

## 📖 Wprowadzenie

W tej ścieżce nauczysz się architektury systemów rozproszonych od podstaw. Każdy temat został zaprojektowany tak, abyś rozumiał **dlaczego** coś robimy, **jak** to testować i **jakie są pułapki**.

### Jak Korzystać z Tej Ścieżki?

1. **Czytaj sekwencyjnie** - tematy budują na sobie
2. **Praktykuj w EventMaster** - każdy temat ma praktyczne zastosowanie
3. **Testuj wszystko** - jako tester, pisz test PRZED implementacją
4. **Pytaj "dlaczego?"** - zrozumienie > zapamiętywanie

---

## 🗺️ Mapa Tematów (50 Punktów)

### **CZĘŚĆ I: Fundamenty (Tematy 1-10)**
Podstawy message-driven i event-driven architecture

### **CZĘŚĆ II: CQRS w Praktyce (Tematy 11-20)**
Command Query Responsibility Segregation

### **CZĘŚĆ III: Event Sourcing (Tematy 21-30)**
Persystencja oparta na zdarzeniach

### **CZĘŚĆ IV: Messaging & Kafka (Tematy 31-40)**
Asynchroniczna komunikacja

### **CZĘŚĆ V: Testowanie & Operacje (Tematy 41-50)**
Praktyczne aspekty dla testera

---

# CZĘŚĆ I: FUNDAMENTY 🏗️

## Temat 1: Co to jest Message-Driven Architecture?

### Definicja (Jak dla 15-latka)

Wyobraź sobie, że masz kilka aplikacji, które muszą ze sobą rozmawiać. Zamiast dzwonić do siebie bezpośrednio (synchronicznie), zostawiają sobie **wiadomości** (messages) na specjalnej tablicy ogłoszeń.

**Analogia ze szkoły:**
- **Synchroniczne (tradycyjne):** Wchodzisz do klasy kolegi i pytasz "Czy masz ołówek?" - musisz czekać na odpowiedź.
- **Message-Driven:** Zostawiasz kartkę na jego ławce "Potrzebuję ołówka" i idziesz dalej. On przeczyta, gdy wróci.

### W EventMaster

```
Frontend (Nuxt) → Wysyła message: "CreateEventCommand"
                ↓
            Kafka (tablica)
                ↓
Backend (Spring) → Odbiera message i tworzy event
```

### Dlaczego To Ważne?

**Problem bez Message-Driven:**
```java
// ❌ BAD: Direct call
eventService.create(event);        // Co jeśli serwis nie działa?
emailService.sendConfirmation();   // Co jeśli email pada?
analyticsService.track();          // Co jeśli analytics jest wolne?
// Wszystko się zawiesza! 😱
```

**Rozwiązanie Message-Driven:**
```java
// ✅ GOOD: Publish message
kafka.publish("CreateEventCommand", event);
return "202 Accepted";  // Natychmiast!
// Email i Analytics obsłużą w tle, asynchronicznie
```

### Testowanie (Perspektywa Testera)

**Test 1: Message jest publikowana**
```java
@Test
void shouldPublishMessageToKafka() {
    // Given
    CreateEventCommand command = new CreateEventCommand(...);
    
    // When
    controller.createEvent(command);
    
    // Then
    assertThat(kafkaTemplate.getHistory())
        .containsMessage("commands.events.create", command);
}
```

**Test 2: System działa mimo awarii konsumenta**
```java
@Test
void shouldNotFailWhenConsumerIsDown() {
    // Given
    stopConsumer("email-service");  // Symuluj awarię
    
    // When
    Response response = controller.createEvent(command);
    
    // Then
    assertThat(response.status()).isEqualTo(202);  // Sukces!
    // Email zostanie wysłany gdy serwis wróci
}
```

### Kluczowe Terminy

- **Message** - pakiet danych wysłany między serwisami
- **Producer** - serwis wysyłający message
- **Consumer** - serwis odbierający message
- **Broker** - pośrednik (Kafka/Redpanda) przechowujący messages

### ⚠️ Pułapki dla Testera

1. **Message może przyjść 2 razy** (at-least-once delivery) - test idempotentności!
2. **Kolejność może się zmienić** - nie zakładaj porządku!
3. **Opóźnienie** - message może przyjść za 5 sekund, nie 5ms

---


## Temat 2: Event-Driven Architecture (EDA)

### Definicja

Event-Driven Architecture to specjalny rodzaj Message-Driven, gdzie messages są **zdarzeniami** (events) - faktami, które już się wydarzyły.

**Różnica kluczowa:**
- **Command** (rozkaz): "CreateEvent" - prosi o akcję
- **Event** (fakt): "EventCreated" - informuje, że coś się stało

### Analogia

**Command:**
- "Zamknij okno!" (możesz odmówić)

**Event:**
- "Okno zostało zamknięte" (już się stało, nie możesz tego cofnąć)

### W EventMaster

```mermaid
graph LR
    A[User] -->|Command| B[CommandController]
    B -->|CreateEventCommand| C[Kafka]
    C -->|Command| D[CommandHandler]
    D -->|Saves to DB| E[PostgreSQL]
    D -->|Event| F[Kafka]
    F -->|EventCreatedEvent| G[Projector]
    G -->|Updates| H[Read Model]
```

**Krok po kroku:**
1. User wysyła **Command**: "CreateEventCommand"
2. Handler wykonuje logikę biznesową
3. Handler publikuje **Event**: "EventCreatedEvent"
4. Inne serwisy reagują na Event (email, analytics, projector)

### Dlaczego Events > Commands?

**Scenario: Utworzenie eventu**

```java
// ❌ BAD: Tylko Command
public void createEvent(CreateEventCommand cmd) {
    Event event = eventRepository.save(cmd.toEntity());
    // Co jeśli ktoś chce wiedzieć, że event powstał?
    // Musimy PAMIĘTAĆ o wywołaniu każdego serwisu!
    emailService.send();
    analyticsService.track();
    // Łatwo coś przeoczyć!
}

// ✅ GOOD: Command + Event
public void createEvent(CreateEventCommand cmd) {
    Event event = eventRepository.save(cmd.toEntity());
    
    // Publish fact
    eventBus.publish(new EventCreatedEvent(event));
    
    // Każdy zainteresowany serwis SAM się zarejestruje!
    // EmailService: @EventListener(EventCreatedEvent.class)
    // AnalyticsService: @EventListener(EventCreatedEvent.class)
}
```

### Testowanie

**Test: Event jest publikowany po zapisie**
```java
@Test
void shouldPublishEventAfterSuccessfulSave() {
    // Given
    CreateEventCommand command = validCommand();
    
    // When
    handler.handle(command);
    
    // Then
    await().atMost(5, SECONDS).untilAsserted(() -> {
        List<EventCreatedEvent> events = 
            testKafkaConsumer.poll("events.lifecycle");
        
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getName())
            .isEqualTo(command.getName());
    });
}
```

**Test: Multiple listeners reagują na ten sam Event**
```java
@Test
void shouldTriggerMultipleListeners() {
    // Given
    EventCreatedEvent event = new EventCreatedEvent(...);
    
    // When
    eventBus.publish(event);
    
    // Then
    await().atMost(10, SECONDS).untilAsserted(() -> {
        // Email został wysłany
        assertThat(emailSpy.getSentEmails()).hasSize(1);
        
        // Analytics został zatrackowany
        assertThat(analyticsSpy.getTrackedEvents()).hasSize(1);
        
        // Projector zaktualizował Read Model
        assertThat(eventViewRepository.findAll()).hasSize(1);
    });
}
```

### Kluczowe Terminy

- **Event** - niezmienialny fakt o czymś, co się wydarzyło
- **Event Bus** - mechanizm dystrybucji events (Kafka)
- **Event Listener** - serwis reagujący na event
- **Event Stream** - uporządkowany ciąg events

### ⚠️ Pułapki

1. **Event NIGDY nie powinien się zmienić** - immutable!
2. **Event powinien zawierać WSZYSTKIE dane** - listener nie powinien pytać API
3. **Nazewnictwo:** PastTense ("EventCreated", nie "CreateEvent")

---

## Temat 3: CQRS - Dlaczego Rozdzielamy Odczyt od Zapisu?

### Definicja

**CQRS** = Command Query Responsibility Segregation (Rozdzielenie Odpowiedzialności Komend i Zapytań)

**Główna idea:** Używamy **różnych modeli** do zapisu danych (Commands) i odczytu danych (Queries).

### Analogia ze Sklepem

**Tradycyjny sklep (bez CQRS):**
- Jedna kasa do wszystkiego
- Klient chce zapłacić → stoi w kolejce
- Klient chce sprawdzić cenę → stoi w tej samej kolejce
- WOLNO! 😞

**Sklep z CQRS:**
- **Kasa** (Write Model) - tylko płatności
- **Infolinia** (Read Model) - tylko sprawdzanie cen
- Klient chce cenę → dzwoni, natychmiast dostaje odpowiedź
- Klient chce zapłacić → idzie do kasy
- SZYBKO! 😊

### W EventMaster

```
┌─────────────────────────────────────────┐
│         WRITE SIDE (Commands)           │
│  ┌────────────────────────────────┐    │
│  │  Table: events                 │    │
│  │  - id                          │    │
│  │  - name                        │    │
│  │  - description                 │    │
│  │  - location                    │    │
│  │  - start_date                  │    │
│  │  - created_by                  │    │
│  │  - status                      │    │
│  └────────────────────────────────┘    │
│         (Normalized, strict)            │
└─────────────────────────────────────────┘
                    │
                    │ EventCreatedEvent
                    ↓
┌─────────────────────────────────────────┐
│          READ SIDE (Queries)            │
│  ┌────────────────────────────────────┐ │
│  │  Table: event_view                │ │
│  │  - id                             │ │
│  │  - name                           │ │
│  │  - short_description (100 chars) │ │
│  │  - city                           │ │
│  │  - country                        │ │
│  │  - event_date (formatted)        │ │
│  │  - organizer_name (denorm!)      │ │
│  │  - ticket_count (denorm!)        │ │
│  │  - is_published                  │ │
│  └────────────────────────────────────┘ │
│    (Denormalized, optimized for reads)  │
└─────────────────────────────────────────┘
```

### Dlaczego To Robimy?

**Problem: E-commerce w Black Friday**

Bez CQRS (jedna baza):
```sql
-- Użytkownik chce ZOBACZYĆ produkt
SELECT p.name, p.price, c.name as category, 
       s.stock, r.avg_rating, COUNT(rev.id) as review_count
FROM products p
JOIN categories c ON p.category_id = c.id
JOIN stock s ON p.id = s.product_id
LEFT JOIN reviews r ON p.id = r.product_id
LEFT JOIN reviews rev ON p.id = rev.product_id
GROUP BY p.id;
-- To trwa 500ms! 😱
-- W Black Friday 100,000 userów robi to jednocześnie
-- Baza danych PADA!
```

Z CQRS (osobny Read Model):
```sql
-- Read Model (materialized view)
SELECT * FROM product_catalog_view WHERE id = ?;
-- To trwa 5ms! ✅
-- Jeden prosty SELECT, wszystkie dane pre-joined
```

### W EventMaster - Praktyczny Przykład

**Scenario:** Strona główna pokazuje listę eventów

**Bez CQRS:**
```java
// ❌ Wolne zapytanie (5+ JOIN-ów)
@GetMapping("/events")
public List<EventDTO> getEvents() {
    return eventRepository.findAll().stream()
        .map(event -> {
            // Dla każdego eventu musimy pobrać:
            User organizer = userRepository.findById(event.getCreatedBy());
            Long ticketCount = ticketRepository.countByEventId(event.getId());
            Long attendeeCount = bookingRepository.countByEventId(event.getId());
            
            return new EventDTO(
                event.getName(),
                organizer.getFullName(),  // N+1 query problem! 😱
                ticketCount,               // N+1 query problem! 😱
                attendeeCount              // N+1 query problem! 😱
            );
        })
        .collect(toList());
}
// Dla 100 eventów = 300 zapytań do bazy! 🐌
```

**Z CQRS:**
```java
// ✅ Szybkie zapytanie (1 SELECT)
@GetMapping("/events")
public List<EventDTO> getEvents() {
    return eventViewRepository.findAll();  // JEDEN SELECT!
    // Wszystkie dane już są w tabeli event_view
}
// Dla 100 eventów = 1 zapytanie! 🚀
```

### Testowanie

**Test 1: Write Model zapisuje poprawnie**
```java
@Test
void writeSide_shouldSaveEventWithNormalizedData() {
    // Given
    CreateEventCommand command = new CreateEventCommand(
        "JavaConf 2025",
        "Awesome conference",
        "Warsaw, Poland",
        LocalDateTime.now()
    );
    
    // When
    handler.handle(command);
    
    // Then
    Event savedEvent = eventRepository.findAll().get(0);
    assertThat(savedEvent.getName()).isEqualTo("JavaConf 2025");
    assertThat(savedEvent.getLocation()).isEqualTo("Warsaw, Poland");
    // Normalized - dokładnie to, co przyszło
}
```

**Test 2: Read Model jest zdenormalizowany**
```java
@Test
void readSide_shouldContainDenormalizedData() {
    // Given
    Event event = createEvent();
    User organizer = createUser("John", "Doe");
    eventBus.publish(new EventCreatedEvent(event, organizer));
    
    // When
    await().atMost(5, SECONDS).untilAsserted(() -> {
        EventView view = eventViewRepository.findById(event.getId()).get();
        
        // Then - denormalized data
        assertThat(view.getOrganizerName()).isEqualTo("John Doe");
        // ☝️ To NIE istnieje w Event entity!
        // Projector połączył dane z User!
    });
}
```

**Test 3: Write i Read są niezależne**
```java
@Test
void writeSide_shouldWorkEvenWhenReadModelFails() {
    // Given
    stopProjector();  // Symuluj awarię Read Side
    
    // When
    handler.handle(command);
    
    // Then
    // Write Model działa!
    assertThat(eventRepository.findAll()).hasSize(1);
    
    // Read Model jest pusty (ale to OK!)
    assertThat(eventViewRepository.findAll()).isEmpty();
    
    // Gdy Projector wróci, uzupełni Read Model
    startProjector();
    await().untilAsserted(() -> 
        assertThat(eventViewRepository.findAll()).hasSize(1)
    );
}
```

### Kluczowe Terminy

- **Write Model** - model zoptymalizowany pod zapis (normalized, strict)
- **Read Model** - model zoptymalizowany pod odczyt (denormalized, fast)
- **Projector** - komponent synchronizujący Read Model na podstawie Events
- **Eventual Consistency** - Read Model jest "w końcu" spójny (może być opóźnienie)

### ⚠️ Pułapki dla Testera

1. **Eventual Consistency** - Read Model może być nieaktualny przez kilka sekund!
   ```java
   // ❌ BAD TEST
   handler.create(event);
   EventView view = readRepository.findById(event.getId());
   // FAIL! View jeszcze nie istnieje!
   
   // ✅ GOOD TEST
   handler.create(event);
   await().atMost(5, SECONDS).untilAsserted(() -> {
       EventView view = readRepository.findById(event.getId());
       assertThat(view).isNotNull();
   });
   ```

2. **Testuj obie strony osobno** - nie mieszaj!
3. **Read Model to cache** - może zostać skasowany i zrekonstruowany

---


## Temat 4: Commands vs Events vs Queries

### Definicje

**Command (Rozkaz):**
- Intent (zamiar) zrobienia czegoś
- Może być odrzucony
- Directed (skierowany do konkretnego handlera)
- Czasownik w trybie rozkazującym: "CreateEvent", "CancelEvent"

**Event (Fakt):**
- Coś, co już się stało
- Nie może być odrzucony (już się wydarzyło!)
- Broadcast (rozgłaszany do wszystkich zainteresowanych)
- Czasownik w czasie przeszłym: "EventCreated", "EventCancelled"

**Query (Zapytanie):**
- Pytanie o dane
- Nie zmienia stanu
- Synchroniczne
- Rzeczownik + "Get": "GetEvent", "ListEvents"

### Analogia: Restauracja

```
Command:  "Poproszę pizzę Margherita"
          (Kucharz może odmówić: "Nie mamy sera")

Event:    "Pizza Margherita została przygotowana"
          (Fakt - kelner, dostawca, kasjer wiedzą)

Query:    "Jakie pizze macie w menu?"
          (Tylko pytanie, nic się nie dzieje)
```

### W EventMaster - Pełny Przepływ

```mermaid
sequenceDiagram
    participant User
    participant API
    participant CommandHandler
    participant WriteDB
    participant Kafka
    participant Projector
    participant ReadDB
    participant QueryAPI

    User->>API: POST /events (Command)
    Note right of User: CreateEventCommand
    API->>Kafka: Publish Command
    Kafka->>CommandHandler: Consume Command
    CommandHandler->>CommandHandler: Validate
    CommandHandler->>WriteDB: Save Event
    WriteDB-->>CommandHandler: OK
    CommandHandler->>Kafka: Publish Event
    Note right of CommandHandler: EventCreatedEvent
    Kafka->>Projector: Consume Event
    Projector->>ReadDB: Update EventView
    
    User->>QueryAPI: GET /events (Query)
    QueryAPI->>ReadDB: SELECT *
    ReadDB-->>QueryAPI: EventView[]
    QueryAPI-->>User: 200 OK + Data
```

### Przykłady w Kodzie

**Command (src/main/java/com/eventmaster/command/CreateEventCommand.java):**
```java
public record CreateEventCommand(
    String name,           // Co chcemy zrobić
    String description,
    String location,
    LocalDateTime startDate,
    UUID requestedBy       // Kto prosi (może być odrzucone!)
) {
    // Command może mieć walidację
    public void validate() {
        if (name == null || name.isBlank()) {
            throw new ValidationException("Name is required");
        }
        if (startDate.isBefore(LocalDateTime.now())) {
            throw new ValidationException("Cannot create event in the past");
        }
    }
}
```

**Event (src/main/java/com/eventmaster/events/EventCreatedEvent.java):**
```java
public record EventCreatedEvent(
    UUID eventId,          // Co się stało
    String name,
    String description,
    String location,
    LocalDateTime startDate,
    UUID createdBy,
    Instant occurredAt     // KIEDY się stało (immutable!)
) {
    // Event NIE MA walidacji - to fakt, który już się wydarzył
    // Event jest IMMUTABLE - wszystkie pola final
}
```

**Query (src/main/java/com/eventmaster/query/ListEventsQuery.java):**
```java
public record ListEventsQuery(
    String city,           // Filtry
    LocalDate from,
    LocalDate to,
    int page,
    int size
) {
    // Query nie zmienia stanu - tylko czyta
}
```

### Kiedy Używać Którego?

| Chcesz...                           | Użyj        | Przykład                    |
|-------------------------------------|-------------|-----------------------------|
| Zmienić stan systemu                | Command     | CreateEvent, CancelBooking  |
| Poinformować o zmianie stanu        | Event       | EventCreated, BookingCancelled |
| Pobrać dane bez zmiany stanu        | Query       | GetEvent, ListEvents        |
| Zareagować na coś, co się stało     | Event       | EventCreated → SendEmail    |

### Testowanie

**Test 1: Command jest walidowany**
```java
@Test
void command_shouldRejectInvalidData() {
    // Given
    CreateEventCommand invalidCommand = new CreateEventCommand(
        "",  // Pusta nazwa
        "desc",
        "Warsaw",
        LocalDateTime.now().minusDays(1),  // W przeszłości
        UUID.randomUUID()
    );
    
    // When / Then
    assertThatThrownBy(() -> handler.handle(invalidCommand))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Name is required");
}
```

**Test 2: Event jest publikowany tylko po sukcesie**
```java
@Test
void event_shouldOnlyBePublishedAfterSuccessfulWrite() {
    // Given
    CreateEventCommand command = validCommand();
    when(eventRepository.save(any())).thenThrow(new DataAccessException("DB Error"));
    
    // When
    assertThatThrownBy(() -> handler.handle(command))
        .isInstanceOf(DataAccessException.class);
    
    // Then
    List<EventCreatedEvent> events = kafkaTestConsumer.poll("events.lifecycle");
    assertThat(events).isEmpty();  // Event NIE został opublikowany! ✅
}
```

**Test 3: Query nie zmienia stanu**
```java
@Test
void query_shouldNotChangeState() {
    // Given
    Long initialCount = eventViewRepository.count();
    ListEventsQuery query = new ListEventsQuery("Warsaw", null, null, 0, 10);
    
    // When
    queryHandler.handle(query);
    queryHandler.handle(query);
    queryHandler.handle(query);  // 3x to samo query
    
    // Then
    Long finalCount = eventViewRepository.count();
    assertThat(finalCount).isEqualTo(initialCount);  // Bez zmian! ✅
}
```

### Kluczowe Terminy

- **Command** - intent do wykonania akcji
- **Event** - immutable fact
- **Query** - request for data
- **Command Handler** - wykonuje logikę biznesową
- **Event Listener** - reaguje na events
- **Query Handler** - zwraca dane z Read Model

### ⚠️ Pułapki

1. **Command NIE może być Event:**
   ```java
   // ❌ BAD
   public void handle(CreateEvent event) { ... }
   // To jest Command, nie Event!
   
   // ✅ GOOD
   public void handle(CreateEventCommand command) {
       // ...
       eventBus.publish(new EventCreatedEvent(...));
   }
   ```

2. **Event MUSI być immutable:**
   ```java
   // ❌ BAD
   public class EventCreatedEvent {
       private String name;
       public void setName(String name) { this.name = name; }  // NO!
   }
   
   // ✅ GOOD
   public record EventCreatedEvent(String name) { }  // Immutable!
   ```

3. **Query nie powinien trafiać do Write Model:**
   ```java
   // ❌ BAD
   public List<Event> listEvents() {
       return eventRepository.findAll();  // Write Model!
   }
   
   // ✅ GOOD
   public List<EventView> listEvents() {
       return eventViewRepository.findAll();  // Read Model!
   }
   ```

---

## Temat 5: Bounded Context (Ograniczony Kontekst)

### Definicja (Domain-Driven Design - Eric Evans)

**Bounded Context** to granica, w której określony model domenowy ma konkretne znaczenie.

**Prościej:** To jak "słownik" dla części systemu. To samo słowo może znaczyć co innego w różnych kontekstach.

### Analogia: Słowo "Książka"

**W Bibliotece:**
- Książka = obiekt fizyczny na półce
- Ma ISBN, stan (nowa/zniszczona), lokalizację

**W Księgarni:**
- Książka = produkt do sprzedaży
- Ma cenę, dostawcę, zapasy

**W Wydawnictwie:**
- Książka = dzieło do opublikowania
- Ma autora, edytora, deadline

To **ta sama** książka, ale **różne modele** w różnych kontekstach!

### W EventMaster - Przykład

```
┌───────────────────────────────────────────────────────┐
│         EVENT MANAGEMENT CONTEXT                      │
│  ┌─────────────────────────────────────────┐         │
│  │  Event                                   │         │
│  │  - id: UUID                              │         │
│  │  - name: String                          │         │
│  │  - description: String                   │         │
│  │  - location: Location                    │         │
│  │  - startDate: LocalDateTime              │         │
│  │  - createdBy: UserId                     │         │
│  │                                           │         │
│  │  behaviors:                               │         │
│  │  - create()                               │         │
│  │  - publish()                              │         │
│  │  - cancel()                               │         │
│  └─────────────────────────────────────────┘         │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│         TICKETING CONTEXT                             │
│  ┌─────────────────────────────────────────┐         │
│  │  Event (różny model!)                    │         │
│  │  - eventId: UUID                         │         │
│  │  - totalCapacity: int                    │         │
│  │  - remainingTickets: int                 │         │
│  │                                           │         │
│  │  TicketType                               │         │
│  │  - name: String (VIP, Standard)          │         │
│  │  - price: Money                           │         │
│  │  - inventory: Inventory                   │         │
│  │                                           │         │
│  │  behaviors:                               │         │
│  │  - reserveTicket()                        │         │
│  │  - releaseReservation()                   │         │
│  └─────────────────────────────────────────┘         │
└───────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────┐
│         ANALYTICS CONTEXT                             │
│  ┌─────────────────────────────────────────┐         │
│  │  Event (jeszcze inny model!)             │         │
│  │  - eventId: UUID                         │         │
│  │  - categoryId: int                       │         │
│  │  - pageViews: long                       │         │
│  │  - conversionRate: BigDecimal            │         │
│  │  - revenue: Money                         │         │
│  │                                           │         │
│  │  behaviors:                               │         │
│  │  - trackView()                            │         │
│  │  - trackPurchase()                        │         │
│  │  - calculateROI()                         │         │
│  └─────────────────────────────────────────┘         │
└───────────────────────────────────────────────────────┘
```

### Dlaczego To Ważne?

**Problem: "God Object" (Obiekt-Bóg)**

Bez Bounded Context próbujemy stworzyć JEDEN model "Event", który robi WSZYSTKO:

```java
// ❌ BAD: God Object
@Entity
public class Event {
    // Event Management
    private String name;
    private String description;
    private Location location;
    
    // Ticketing
    private List<TicketType> ticketTypes;
    private int remainingCapacity;
    private Money totalRevenue;
    
    // Analytics
    private long pageViews;
    private BigDecimal conversionRate;
    private Map<LocalDate, Integer> dailyViews;
    
    // Payment
    private String stripeAccountId;
    private List<Payout> payouts;
    
    // Marketing
    private List<EmailCampaign> campaigns;
    private SocialMediaLinks socialMedia;
    
    // ... 50 more fields 😱
}
// Klasa ma 2000 linii kodu!
// Nikt nie rozumie co ona robi!
// Każda zmiana łamie coś innego!
```

**Rozwiązanie: Oddzielne modele w Bounded Contexts**

```java
// ✅ GOOD: Event Management Context
package com.eventmaster.eventmanagement;

@Entity
public class Event {
    private UUID id;
    private String name;
    private String description;
    private Location location;
    private LocalDateTime startDate;
    
    // TYLKO logika zarządzania eventem
    public void publish() { ... }
    public void cancel() { ... }
}

// ✅ GOOD: Ticketing Context
package com.eventmaster.ticketing;

@Entity
public class Event {  // To jest INNY Event!
    private UUID eventId;  // ID z Event Management Context
    private int totalCapacity;
    
    @OneToMany
    private List<TicketType> ticketTypes;
    
    // TYLKO logika biletowa
    public ReservationResult reserveTickets(int quantity) { ... }
}

// ✅ GOOD: Analytics Context
package com.eventmaster.analytics;

@Document  // Może być nawet inna baza (MongoDB)!
public class EventAnalytics {
    private UUID eventId;
    private long pageViews;
    private Map<LocalDate, Metrics> dailyMetrics;
    
    // TYLKO logika analityczna
    public void trackView() { ... }
    public BigDecimal calculateROI() { ... }
}
```

### Komunikacja Między Kontekstami

**Konteksty NIE dzielą się bazą danych!** Komunikują się przez **Events**.

```java
// Event Management Context
public class EventCommandHandler {
    public void handle(CreateEventCommand cmd) {
        Event event = new Event(cmd);
        eventRepository.save(event);
        
        // Publish event dla innych kontekstów
        eventBus.publish(new EventCreatedEvent(
            event.getId(),
            event.getName(),
            event.getCapacity()
        ));
    }
}

// Ticketing Context (słucha)
@KafkaListener(topics = "events.lifecycle")
public class TicketingProjector {
    public void on(EventCreatedEvent event) {
        // Tworzymy NASZ model Event w ticketing
        TicketingEvent ticketEvent = new TicketingEvent(
            event.getEventId(),
            event.getCapacity()
        );
        ticketingRepository.save(ticketEvent);
    }
}
```

### Testowanie

**Test 1: Każdy kontekst ma własną bazę**
```java
@Test
void eventManagementContext_shouldHaveOwnDatabase() {
    // Given
    CreateEventCommand cmd = new CreateEventCommand(...);
    
    // When
    eventManagementHandler.handle(cmd);
    
    // Then
    // Event Management DB
    Event managementEvent = eventRepository.findAll().get(0);
    assertThat(managementEvent.getName()).isNotNull();
    assertThat(managementEvent.getTicketTypes()).isNull();  // Nie ma! ✅
    
    // Ticketing DB (osobna!)
    assertThat(ticketingRepository.findAll()).isEmpty();  // Jeszcze pusty
}
```

**Test 2: Komunikacja przez Events**
```java
@Test
void contexts_shouldCommunicateThroughEvents() {
    // Given
    UUID eventId = UUID.randomUUID();
    
    // When - Event Management publikuje
    eventBus.publish(new EventCreatedEvent(eventId, "JavaConf", 500));
    
    // Then - Ticketing reaguje
    await().atMost(5, SECONDS).untilAsserted(() -> {
        TicketingEvent ticketEvent = 
            ticketingRepository.findByEventId(eventId).get();
        assertThat(ticketEvent.getTotalCapacity()).isEqualTo(500);
    });
}
```

**Test 3: Failure isolation (izolacja awarii)**
```java
@Test
void eventManagementContext_shouldWorkEvenWhenTicketingFails() {
    // Given
    stopContext("ticketing");  // Symuluj awarię Ticketing Context
    
    // When
    eventManagementHandler.handle(cmd);
    
    // Then
    // Event Management działa!
    assertThat(eventRepository.findAll()).hasSize(1);
    
    // Ticketing nie dostał eventu (ale to OK - dostanie później)
    assertThat(ticketingRepository.findAll()).isEmpty();
}
```

### Kluczowe Terminy

- **Bounded Context** - logiczna granica, w której model ma konkretne znaczenie
- **Context Map** - mapa pokazująca relacje między kontekstami
- **Shared Kernel** - wspólna część (np. EventId, Money) między kontekstami
- **Anti-Corruption Layer** - warstwa tłumacząca między kontekstami

### ⚠️ Pułapki dla Testera

1. **Nie testuj przez granice kontekstów!**
   ```java
   // ❌ BAD
   Event event = eventManagementRepo.findById(id);
   TicketingEvent ticket = ticketingRepo.findByEventId(event.getId());
   // To łamie enkapsulację!
   
   // ✅ GOOD
   // Test Event Management osobno
   // Test Ticketing osobno
   // Test integracji przez Events
   ```

2. **Eventual Consistency między kontekstami**
   - Event Management może mieć event
   - Ticketing jeszcze nie (opóźnienie Kafki)
   - ZAWSZE używaj `await()` w testach!

3. **Różne bazy danych**
   - Transakcje NIE działają między kontekstami
   - Musisz obsłużyć częściowe failures

---


## Temat 6: Aggregate (Agregat) - Granica Spójności

### Definicja
**Aggregate** to klaster powiązanych obiektów traktowanych jako jednostka transakcyjna. Ma Aggregate Root (główny obiekt) kontrolujący dostęp.

### W EventMaster
```java
@Entity
public class Event {  // Aggregate Root
    @Id private UUID id;
    private String name;
    @Embedded private Location location;  // Part of aggregate
    @OneToMany private List<EventDate> dates;  // Part of aggregate
    
    public void publish() { /* Invariants */ }
}
```

### Testowanie
```java
@Test
void aggregate_shouldEnforceInvariants() {
    Event event = new Event();
    assertThatThrownBy(() -> event.publish())
        .hasMessageContaining("Location is required");
}
```

---

## Temat 7: Domain Events - Język Biznesu

### Definicja
Domain Events to wydarzenia ważne dla biznesu, nazwane językiem eksperckim domeny.

### W EventMaster
```java
// ✅ GOOD: Język biznesu
record EventPublishedEvent(UUID eventId, Instant publishedAt) {}
record TicketSoldOutEvent(UUID eventId, String ticketType) {}

// ❌ BAD: Techniczny język
record EventStatusChangedEvent(UUID id, int status) {}  // Co to znaczy?
```

### Testowanie
```java
@Test
void domainEvent_shouldHaveBusinessMeaning() {
    Event event = new Event();
    event.publish();
    
    DomainEvent emittedEvent = event.pollEvent();
    assertThat(emittedEvent).isInstanceOf(EventPublishedEvent.class);
}
```

---

## Temat 8: Eventual Consistency - Spójność Ostateczna

### Definicja
System NIE jest spójny natychmiast, ale **w końcu** wszystkie części będą spójne.

### Analogia
Bank przelewa pieniądze:
- Write Model: Przelew wykonany (natychmiast)
- Read Model (wyciąg): Pojawi się za kilka sekund
- Eventual: Za 5 sekund = spójny

### W EventMaster
```java
// Write: Event zapisany
handler.create(event);

// Read: event_view jeszcze nie istnieje (!)
// Za 2-5 sekund: Projector zaktualizuje
```

### Testowanie
```java
@Test
void eventualConsistency_readModelEventuallyConsistent() {
    handler.create(event);
    
    // ❌ Fail immediately
    // assertThat(readRepo.findById(id)).isPresent();
    
    // ✅ Wait for consistency
    await().atMost(5, SECONDS).untilAsserted(() ->
        assertThat(readRepo.findById(id)).isPresent()
    );
}
```

### ⚠️ Pułapka
**User experience:** "Nie widzę mojego eventu!" - trzeba pokazać feedback UI.

---

## Temat 9: Idempotency - Wielokrotne Wykonanie = Jeden Efekt

### Definicja
Operacja **idempotentna** daje ten sam efekt niezależnie od ile razy ją wykonasz.

### Analogia
- Idempotentne: Włącznik światła (klik 5x = światło ON)
- NIE idempotentne: Przycisk "Dodaj 10 PLN" (klik 5x = +50 PLN!)

### W EventMaster
```java
// ❌ BAD: Not idempotent
@KafkaListener
void handle(EventCreatedEvent event) {
    eventView.incrementViewCount();  // Kafka może dostarczyć 2x!
}

// ✅ GOOD: Idempotent
@KafkaListener
void handle(EventCreatedEvent event) {
    if (processedEvents.contains(event.getEventId())) {
        return;  // Already processed
    }
    eventView.create(event);
    processedEvents.add(event.getEventId());
}
```

### Testowanie
```java
@Test
void handler_shouldBeIdempotent() {
    EventCreatedEvent event = new EventCreatedEvent(...);
    
    projector.handle(event);
    projector.handle(event);  // 2x the same event
    projector.handle(event);  // 3x the same event
    
    assertThat(eventViewRepository.findAll()).hasSize(1);  // ✅ Only 1!
}
```

---

## Temat 10: Saga Pattern - Długo Działające Transakcje

### Definicja
**Saga** to sekwencja lokalnych transakcji koordynowanych przez events lub komendy.

### Analogia: Rezerwacja Podróży
1. Zarezerwuj lot → OK
2. Zarezerwuj hotel → FAIL!
3. **Compensate:** Anuluj lot (rollback)

### W EventMaster - Przykład: Booking Saga
```
1. Command: CreateBookingCommand
2. Event: BookingCreatedEvent
3. Command: ReserveTicketsCommand
4. Event: TicketsReservedEvent (OK) lub TicketsUnavailableEvent (FAIL)
5. If FAIL: Command: CancelBookingCommand
```

### Testowanie
```java
@Test
void saga_shouldCompensateOnFailure() {
    // Step 1: Booking created
    sagaOrchestrator.start(new CreateBookingCommand(...));
    await().until(() -> bookingRepo.findAll().size() == 1);
    
    // Step 2: Tickets unavailable (simulated)
    simulateTicketFailure();
    
    // Step 3: Saga compensates
    await().until(() -> bookingRepo.findAll().get(0).isCancelled());
}
```

---

# CZĘŚĆ II: CQRS W PRAKTYCE (Tematy 11-20)

## Temat 11: Projections - Budowanie Read Model

### Definicja
**Projection** to proces tworzenia Read Model z Event Stream.

### W EventMaster
```java
@KafkaListener(topics = "events.lifecycle")
public class EventViewProjector {
    @Transactional
    public void on(EventCreatedEvent event) {
        EventView view = new EventView(
            event.getEventId(),
            event.getName(),
            event.getLocation(),
            "PUBLISHED"  // Denormalized status
        );
        repository.save(view);
    }
    
    @Transactional
    public void on(EventCancelledEvent event) {
        EventView view = repository.findById(event.getEventId()).get();
        view.setStatus("CANCELLED");
        repository.save(view);
    }
}
```

### Testowanie
```java
@Test
void projection_shouldBuildReadModel() {
    eventBus.publish(new EventCreatedEvent(...));
    
    await().untilAsserted(() -> {
        EventView view = repo.findAll().get(0);
        assertThat(view.getStatus()).isEqualTo("PUBLISHED");
    });
}
```

---

## Temat 12: Multiple Read Models - Różne Widoki

### Definicja
Możesz mieć **wiele Read Models** zoptymalizowanych pod różne use cases.

### W EventMaster
```
EventCreatedEvent
    ↓
    ├─→ EventListView (lista: name, date, city)
    ├─→ EventDetailView (szczegóły: pełny opis, organizator)
    ├─→ EventSearchView (Elasticsearch: full-text search)
    └─→ EventAnalyticsView (analytics: views, conversions)
```

### Testowanie
```java
@Test
void multipleProjections_shouldCreateDifferentViews() {
    eventBus.publish(new EventCreatedEvent(...));
    
    await().untilAsserted(() -> {
        assertThat(listViewRepo.findAll()).hasSize(1);
        assertThat(detailViewRepo.findAll()).hasSize(1);
        assertThat(searchViewRepo.findAll()).hasSize(1);
    });
}
```

---

## Temat 13: Rebuilding Projections - Odbudowa Read Model

### Definicja
Read Model można skasować i zbudować od nowa z Event Stream.

### W EventMaster
```java
public class ProjectionRebuilder {
    public void rebuildEventView() {
        eventViewRepo.deleteAll();  // Kasuj stary
        
        List<DomainEvent> events = eventStore.getAllEvents();
        events.forEach(event -> projector.handle(event));  // Odbuduj
    }
}
```

### Testowanie
```java
@Test
void projection_canBeRebuilt() {
    // Given: 100 events
    publishEvents(100);
    await().until(() -> repo.count() == 100);
    
    // When: Rebuild
    repo.deleteAll();
    rebuilder.rebuildEventView();
    
    // Then: 100 events again
    await().until(() -> repo.count() == 100);
}
```

---

## Temat 14: Optimistic Concurrency Control - Wersjonowanie

### Definicja
Używamy **version field** do wykrywania konfliktów współbieżnych.

### W EventMaster
```java
@Entity
public class Event {
    @Id private UUID id;
    @Version private Long version;  // Auto-increment on save
    private String name;
}

// User 1 i User 2 edytują ten sam event
// User 1 zapisuje → version = 2
// User 2 próbuje zapisać (ma version = 1) → OptimisticLockException!
```

### Testowanie
```java
@Test
void optimisticLocking_shouldDetectConflict() {
    UUID id = createEvent().getId();
    
    Event event1 = repo.findById(id).get();
    Event event2 = repo.findById(id).get();
    
    event1.setName("Name A");
    repo.save(event1);  // OK, version = 2
    
    event2.setName("Name B");
    assertThatThrownBy(() -> repo.save(event2))
        .isInstanceOf(OptimisticLockException.class);
}
```

---

## Temat 15: Command Validation - Walidacja Komend

### Definicja
Commands są walidowane **przed** wykonaniem logiki biznesowej.

### W EventMaster
```java
public record CreateEventCommand(String name, LocalDateTime date) {
    public CreateEventCommand {  // Compact constructor
        if (name == null || name.isBlank()) {
            throw new ValidationException("Name required");
        }
        if (date.isBefore(LocalDateTime.now())) {
            throw new ValidationException("Date in past");
        }
    }
}
```

### Testowanie
```java
@Test
void command_shouldValidateInput() {
    assertThatThrownBy(() -> 
        new CreateEventCommand("", LocalDateTime.now())
    ).isInstanceOf(ValidationException.class);
}
```

---

## Temat 16: Command Handler Pattern - Obsługa Komend

### Definicja
Każdy Command ma dedykowany Handler wykonujący logikę.

### W EventMaster
```java
@Service
public class CreateEventCommandHandler {
    public void handle(CreateEventCommand cmd) {
        cmd.validate();
        
        Event event = new Event(cmd.name(), cmd.date());
        repository.save(event);
        
        eventBus.publish(new EventCreatedEvent(event.getId()));
    }
}
```

### Testowanie
```java
@Test
void handler_shouldProcessCommand() {
    CreateEventCommand cmd = new CreateEventCommand("JavaConf", tomorrow);
    
    handler.handle(cmd);
    
    assertThat(repository.findAll()).hasSize(1);
}
```

---

## Temat 17: Query Handler Pattern - Obsługa Zapytań

### Definicja
Każde Query ma dedykowany Handler zwracający dane z Read Model.

### W EventMaster
```java
@Service
public class ListEventsQueryHandler {
    public List<EventViewDTO> handle(ListEventsQuery query) {
        return repository.findByCity(query.city())
            .stream()
            .map(this::toDTO)
            .collect(toList());
    }
}
```

### Testowanie
```java
@Test
void queryHandler_shouldReturnFilteredData() {
    createEvent("Warsaw");
    createEvent("Krakow");
    
    ListEventsQuery query = new ListEventsQuery("Warsaw");
    List<EventViewDTO> result = handler.handle(query);
    
    assertThat(result).hasSize(1);
    assertThat(result.get(0).city()).isEqualTo("Warsaw");
}
```

---

## Temat 18: Denormalization - Duplikacja Danych dla Szybkości

### Definicja
W Read Model **duplikujemy** dane z różnych źródeł dla szybszego odczytu.

### W EventMaster
```java
// Write Model: Normalized
Event { id, name, createdBy }  // createdBy = UUID reference
User { id, firstName, lastName }

// Read Model: Denormalized
EventView { 
    id, 
    name, 
    organizerFullName  // "John Doe" - już połączone!
}
```

### Testowanie
```java
@Test
void readModel_shouldContainDenormalizedData() {
    User user = createUser("John", "Doe");
    Event event = createEvent(user.getId());
    
    eventBus.publish(new EventCreatedEvent(event, user));
    
    await().untilAsserted(() -> {
        EventView view = repo.findById(event.getId()).get();
        assertThat(view.getOrganizerFullName()).isEqualTo("John Doe");
    });
}
```

---

## Temat 19: Materialized Views - Prekalkulowane Widoki

### Definicja
**Materialized View** to wynik zapytania zapisany jako tabela (pre-computed).

### W EventMaster
```sql
-- Zamiast tego (wolne):
SELECT e.id, COUNT(b.id) as booking_count
FROM events e
LEFT JOIN bookings b ON e.id = b.event_id
GROUP BY e.id;

-- Mamy to (szybkie):
SELECT * FROM event_statistics_view;
```

### Testowanie
```java
@Test
void materializedView_shouldBeUpdatedOnEvent() {
    Event event = createEvent();
    createBooking(event.getId());
    createBooking(event.getId());
    
    eventBus.publish(new BookingCreatedEvent(...));
    
    await().untilAsserted(() -> {
        EventStats stats = statsRepo.findByEventId(event.getId()).get();
        assertThat(stats.getBookingCount()).isEqualTo(2);
    });
}
```

---

## Temat 20: Snapshot Pattern - Zrzuty Stanu

### Definicja
Zamiast odtwarzać stan z 10,000 events, robimy **snapshot** co 100 events.

### W EventMaster (Future)
```java
@Entity
public class EventSnapshot {
    private UUID eventId;
    private Long version;  // Event #100, #200, #300...
    private byte[] state;  // Serialized Event state
}

// Odtwarzanie:
EventSnapshot snapshot = repo.findLatestSnapshot(eventId);  // #900
List<Event> events = eventStore.getEventsAfter(eventId, 900);  // #901-#1000
Event current = snapshot.deserialize();
events.forEach(e -> current.apply(e));  // Tylko 100 events, nie 1000!
```

---

# CZĘŚĆ III: EVENT SOURCING (Tematy 21-30)

## Temat 21: Event Sourcing - Persystencja przez Wydarzenia

### Definicja
Zamiast zapisywać **obecny stan**, zapisujemy **wszystkie wydarzenia**, które doprowadziły do tego stanu.

### Analogia
**Tradycyjnie (State-based):**
- Konto bankowe: saldo = 1000 PLN

**Event Sourcing:**
- Wpłata +500 PLN (10:00)
- Wypłata -200 PLN (11:00)
- Wpłata +700 PLN (12:00)
- **Oblicz:** 500 - 200 + 700 = 1000 PLN

### W EventMaster (Future)
```java
// Event Store
@Entity
public class EventStoreEntry {
    @Id private UUID id;
    private UUID aggregateId;  // Event ID
    private Long version;
    private String eventType;  // "EventCreatedEvent"
    private String payload;    // JSON
    private Instant occurredAt;
}

// Odtwarzanie Event
public Event reconstruct(UUID eventId) {
    List<DomainEvent> events = eventStore.getEvents(eventId);
    Event event = new Event();
    events.forEach(e -> event.apply(e));  // Replay!
    return event;
}
```

### Testowanie
```java
@Test
void eventSourcing_shouldReconstructState() {
    UUID eventId = UUID.randomUUID();
    
    eventStore.append(eventId, new EventCreatedEvent(...));
    eventStore.append(eventId, new EventPublishedEvent(...));
    eventStore.append(eventId, new EventCancelledEvent(...));
    
    Event reconstructed = eventSourcingRepo.reconstruct(eventId);
    
    assertThat(reconstructed.getStatus()).isEqualTo(CANCELLED);
}
```

---

## Temat 22: Event Store - Magazyn Zdarzeń

### Definicja
**Event Store** to append-only database przechowująca wszystkie domain events.

### Charakterystyka
- Append-only (tylko dodawanie, nigdy usuwanie/edycja)
- Ordered (zachowana kolejność)
- Immutable (events nigdy się nie zmieniają)

### Testowanie
```java
@Test
void eventStore_shouldBeAppendOnly() {
    UUID aggregateId = UUID.randomUUID();
    
    eventStore.append(aggregateId, new EventCreatedEvent(...));
    eventStore.append(aggregateId, new EventPublishedEvent(...));
    
    List<DomainEvent> events = eventStore.getEvents(aggregateId);
    assertThat(events).hasSize(2);
    assertThat(events.get(0)).isInstanceOf(EventCreatedEvent.class);
    assertThat(events.get(1)).isInstanceOf(EventPublishedEvent.class);
}
```

---

## Temat 23: Temporal Queries - Podróże w Czasie

### Definicja
**Temporal Query** to zapytanie pozwalające zobaczyć, jak wyglądał stan systemu w przeszłości.

### Analogia
Wyciąg bankowy pozwala zobaczyć:
- Saldo dzisiaj: 1000 PLN
- Saldo 3 miesiące temu: 1500 PLN
- Wszystkie operacje między nimi

### W EventMaster - Przykład
```java
public class TemporalEventRepository {
    public Event getEventStateAt(UUID eventId, Instant timestamp) {
        // Pobierz wszystkie eventy do określonego czasu
        List<DomainEvent> events = eventStore
            .getEvents(eventId)
            .stream()
            .filter(e -> e.getOccurredAt().isBefore(timestamp))
            .collect(toList());
        
        // Odtwórz stan
        Event event = new Event();
        events.forEach(e -> event.apply(e));
        return event;
    }
}
```

### Use Cases
**1. Audyt:** "Kto zmienił nazwę eventu 2 tygodnie temu?"
```java
Event eventTwoWeeksAgo = repo.getEventStateAt(
    eventId, 
    Instant.now().minus(14, DAYS)
);
```

**2. Debugging:** "Dlaczego event był anulowany?"
```java
// Zobacz wszystkie wydarzenia tego dnia
List<DomainEvent> eventsOnThatDay = eventStore
    .getEventsInTimeRange(eventId, morningStart, eveningEnd);
eventsOnThatDay.forEach(System.out::println);
// Output:
// 10:00 - EventCreatedEvent
// 14:30 - TicketsSoldOutEvent
// 16:45 - VenueCancelledEvent  // ← Tutaj!
// 16:46 - EventCancelledEvent
```

**3. Compliance:** "Pokaż stan konta użytkownika w momencie transakcji"

### Testowanie
```java
@Test
void temporalQuery_shouldReturnHistoricalState() {
    // Given: Sekwencja zmian
    UUID eventId = UUID.randomUUID();
    Instant t1 = Instant.now();
    
    eventStore.append(eventId, new EventCreatedEvent(eventId, "JavaConf"));
    Thread.sleep(1000);
    Instant t2 = Instant.now();
    
    eventStore.append(eventId, new EventRenamedEvent(eventId, "JavaConf 2025"));
    Thread.sleep(1000);
    Instant t3 = Instant.now();
    
    // When: Query at t2 (po create, przed rename)
    Event stateAtT2 = repo.getEventStateAt(eventId, t2);
    
    // Then
    assertThat(stateAtT2.getName()).isEqualTo("JavaConf");  // Old name!
    
    // When: Query at t3 (po rename)
    Event stateAtT3 = repo.getEventStateAt(eventId, t3);
    
    // Then
    assertThat(stateAtT3.getName()).isEqualTo("JavaConf 2025");  // New name!
}
```

### Implementacja w PostgreSQL 18
```sql
-- Event Store Table
CREATE TABLE event_store (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_aggregate_version UNIQUE (aggregate_id, version)
);

-- Index dla temporal queries
CREATE INDEX idx_event_store_temporal 
ON event_store (aggregate_id, occurred_at);

-- Query: Stan eventu o 14:00 dnia 2025-01-15
SELECT * FROM event_store
WHERE aggregate_id = 'uuid-here'
  AND occurred_at <= '2025-01-15 14:00:00'
ORDER BY version ASC;
```

### ⚠️ Pułapki
1. **Performance:** Jeśli masz 100,000 events dla jednego aggregate, temporal query będzie wolny
   - **Rozwiązanie:** Snapshots co 1000 events
2. **Timezone:** ZAWSZE używaj UTC (PostgreSQL: TIMESTAMPTZ)
3. **Precision:** Milliseconds mogą nie wystarczyć przy wysokim throughput

---

## Temat 24: Audit Log - Pełna Historia Zmian

### Definicja
**Audit Log** to niezmienna historia wszystkich operacji w systemie, wymagana dla compliance (GDPR, SOX, HIPAA).

### Dlaczego To Ważne?
**Bez Event Sourcing:**
```java
// ❌ Utrata informacji
Event event = eventRepository.findById(id);
event.setName("New Name");  // Stara nazwa jest STRACONA!
eventRepository.save(event);
```

**Z Event Sourcing:**
```java
// ✅ Pełna historia
eventStore.append(new EventCreatedEvent(..., "Old Name"));
eventStore.append(new EventRenamedEvent(..., "New Name"));
// Oba fakty są zachowane!
```

### W EventMaster - Audit Dashboard
```java
@RestController
@RequestMapping("/api/v1/audit")
public class AuditController {
    
    @GetMapping("/events/{eventId}/history")
    public List<AuditEntry> getEventHistory(@PathVariable UUID eventId) {
        return eventStore.getEvents(eventId)
            .stream()
            .map(event -> new AuditEntry(
                event.getOccurredAt(),
                event.getClass().getSimpleName(),
                extractUser(event),
                extractChanges(event)
            ))
            .collect(toList());
    }
}

// Response:
[
  {
    "timestamp": "2025-01-15T10:00:00Z",
    "action": "EventCreatedEvent",
    "user": "john.doe@example.com",
    "changes": { "name": "JavaConf", "location": "Warsaw" }
  },
  {
    "timestamp": "2025-01-15T14:30:00Z",
    "action": "EventPublishedEvent",
    "user": "admin@example.com",
    "changes": { "status": "PUBLISHED" }
  },
  {
    "timestamp": "2025-01-16T09:00:00Z",
    "action": "EventCancelledEvent",
    "user": "john.doe@example.com",
    "changes": { "status": "CANCELLED", "reason": "Venue unavailable" }
  }
]
```

### Compliance Requirements
**GDPR Art. 15:** "Prawo dostępu do danych"
```java
@Test
void auditLog_shouldProvideUserDataAccess() {
    // User request: "Pokaż wszystkie moje dane"
    UUID userId = UUID.fromString("user-123");
    
    List<DomainEvent> userEvents = eventStore
        .getAllEvents()
        .stream()
        .filter(e -> e.getUserId().equals(userId))
        .collect(toList());
    
    // Generate GDPR report
    GDPRReport report = gdprService.generateReport(userEvents);
    assertThat(report.getEvents()).isNotEmpty();
}
```

**SOX (Sarbanes-Oxley):** "Niezmienność danych finansowych"
```java
@Test
void auditLog_shouldBeImmutable() {
    UUID eventId = UUID.randomUUID();
    eventStore.append(eventId, new TicketPurchasedEvent(..., Money.of(100, "PLN")));
    
    // Próba modyfikacji event
    assertThatThrownBy(() -> 
        eventStore.updateEvent(eventId, 0, new TicketPurchasedEvent(..., Money.of(50, "PLN")))
    ).isInstanceOf(UnsupportedOperationException.class)
     .hasMessageContaining("Event Store is append-only");
}
```

### Testowanie
```java
@Test
void auditLog_shouldTrackAllChanges() {
    // Given
    UUID eventId = UUID.randomUUID();
    UUID userId = UUID.randomUUID();
    
    // When: Sekwencja operacji
    commandBus.send(new CreateEventCommand(eventId, userId, "JavaConf"));
    commandBus.send(new RenameEventCommand(eventId, userId, "JavaConf 2025"));
    commandBus.send(new PublishEventCommand(eventId, userId));
    commandBus.send(new CancelEventCommand(eventId, userId, "Venue issue"));
    
    // Then: Audit log zawiera wszystko
    List<AuditEntry> auditLog = auditService.getHistory(eventId);
    
    assertThat(auditLog).hasSize(4);
    assertThat(auditLog.get(0).getAction()).isEqualTo("CREATE");
    assertThat(auditLog.get(1).getAction()).isEqualTo("RENAME");
    assertThat(auditLog.get(2).getAction()).isEqualTo("PUBLISH");
    assertThat(auditLog.get(3).getAction()).isEqualTo("CANCEL");
    
    // Wszystkie operacje tego samego usera
    assertThat(auditLog).allMatch(e -> e.getUserId().equals(userId));
}
```

### PostgreSQL 18 - Audit Table Pattern
```sql
-- Dedykowana tabela audit (redundancja dla szybkiego dostępu)
CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    action VARCHAR(100) NOT NULL,
    user_id UUID NOT NULL,
    user_email VARCHAR(255) NOT NULL,
    ip_address INET,
    changes JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_store_id UUID NOT NULL REFERENCES event_store(id)
);

CREATE INDEX idx_audit_log_aggregate ON audit_log (aggregate_id, occurred_at);
CREATE INDEX idx_audit_log_user ON audit_log (user_id, occurred_at);

-- Trigger: Auto-populate audit_log from event_store
CREATE OR REPLACE FUNCTION fn_audit_log_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (
        aggregate_id, aggregate_type, action, user_id, user_email,
        changes, occurred_at, event_store_id
    ) VALUES (
        NEW.aggregate_id,
        NEW.aggregate_type,
        NEW.event_type,
        (NEW.event_data->>'userId')::UUID,
        NEW.event_data->>'userEmail',
        NEW.event_data,
        NEW.occurred_at,
        NEW.id
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_log
AFTER INSERT ON event_store
FOR EACH ROW EXECUTE FUNCTION fn_audit_log_trigger();
```

### ⚠️ Pułapki
1. **Storage:** Audit log rośnie bez końca - potrzebne archiwizowanie
2. **Performance:** Indexy na JSONB mogą być wolne - używaj dedykowanych kolumn dla często wyszukiwanych pól
3. **Privacy:** Nie loguj wrażliwych danych (hasła, karty kredytowe)

---

## Temat 25: Debugging z Event Replay

### Definicja
**Event Replay** to technika debugowania przez "odtworzenie" sekwencji events w środowisku testowym.

### Dlaczego To Game-Changer dla Testera?

**Tradycyjny debugging:**
```
Bug w production
  ↓
"U mnie działa" 🤷
  ↓
Nie można odtworzyć
  ↓
NIEZAŁATANY BUG
```

**Event Replay:**
```
Bug w production
  ↓
Export events z production
  ↓
Replay w dev environment
  ↓
BUG ODTWORZONY! 🎉
  ↓
FIX & TEST
```

### W EventMaster - Praktyczny Przykład

**Scenario:** User zgłasza: "Mój event zniknął po publikacji!"

**Krok 1: Export events z production**
```java
@RestController
@RequestMapping("/api/v1/debug")
public class EventReplayController {
    
    @GetMapping("/events/{eventId}/export")
    @PreAuthorize("hasRole('ADMIN')")
    public EventStreamExport exportEvents(@PathVariable UUID eventId) {
        List<DomainEvent> events = eventStore.getEvents(eventId);
        return new EventStreamExport(eventId, events);
    }
}

// Response:
{
  "eventId": "uuid-123",
  "events": [
    {
      "type": "EventCreatedEvent",
      "data": { "name": "JavaConf", ... },
      "occurredAt": "2025-01-15T10:00:00Z",
      "version": 1
    },
    {
      "type": "EventPublishedEvent",
      "data": { "publishedAt": "2025-01-15T14:00:00Z" },
      "occurredAt": "2025-01-15T14:00:05Z",
      "version": 2
    },
    {
      "type": "EventDeletedEvent",  // ← O! Bug jest tutaj!
      "data": { "reason": "Auto-cleanup" },
      "occurredAt": "2025-01-15T14:00:07Z",
      "version": 3
    }
  ]
}
```

**Krok 2: Replay w dev**
```java
@Service
public class EventReplayService {
    
    @Transactional
    public void replay(EventStreamExport export) {
        // Clear dev database
        eventRepository.deleteAll();
        eventViewRepository.deleteAll();
        
        // Replay events one by one
        export.getEvents().forEach(event -> {
            log.info("Replaying: {} at {}", 
                event.getType(), event.getOccurredAt());
            
            // Simulate original timing (optional)
            if (export.isReplayWithTiming()) {
                waitForOriginalDelay(event);
            }
            
            eventBus.publish(event);
        });
    }
}
```

**Krok 3: Debug**
```java
@Test
void debug_eventDisappearsAfterPublish() {
    // Given: Event stream from production
    EventStreamExport productionEvents = loadFromFile("bug-123.json");
    
    // When: Replay
    replayService.replay(productionEvents);
    
    // Then: Verify each step
    await().untilAsserted(() -> {
        // After EventCreatedEvent
        assertThat(eventRepository.findAll()).hasSize(1);
        
        // After EventPublishedEvent
        Event event = eventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(PUBLISHED);
        
        // After EventDeletedEvent - HERE'S THE BUG!
        // Event is deleted by auto-cleanup rule (shouldn't happen for published events)
        assertThat(eventRepository.findAll()).isEmpty();  // Bug confirmed!
    });
}
```

**Krok 4: Fix**
```java
// ❌ BAD: Auto-cleanup deletes everything old
@Scheduled(fixedDelay = 3600000)  // Every hour
public void autoCleanup() {
    eventRepository.deleteOlderThan(Instant.now().minus(2, SECONDS));
    // Bug: Deletes published events too!
}

// ✅ GOOD: Don't delete published events
@Scheduled(fixedDelay = 3600000)
public void autoCleanup() {
    eventRepository.deleteOlderThan(
        Instant.now().minus(2, SECONDS),
        List.of(EventStatus.DRAFT)  // Only drafts!
    );
}
```

### Advanced: Time-Travel Debugging
```java
@Test
void debug_withBreakpoints() {
    EventStreamExport events = loadProductionEvents();
    
    // Replay with breakpoints
    replayService.replayWithBreakpoints(events, List.of(
        breakpoint().after("EventPublishedEvent").inspect(state -> {
            log.info("State after publish: {}", state);
            assertThat(state.getEvent().getStatus()).isEqualTo(PUBLISHED);
        }),
        breakpoint().before("EventDeletedEvent").inspect(state -> {
            log.error("About to delete! Current state: {}", state);
            // Debugger stops here - inspect why deletion happens
        })
    ));
}
```

### Testowanie
```java
@Test
void eventReplay_shouldReconstructExactState() {
    // Given: Original sequence
    UUID eventId = UUID.randomUUID();
    Event originalEvent = createEventWithHistory(eventId);
    EventStreamExport export = exportService.export(eventId);
    
    // When: Replay in clean environment
    cleanDatabase();
    replayService.replay(export);
    
    // Then: State is identical
    Event replayedEvent = eventRepository.findById(eventId).get();
    assertThat(replayedEvent).isEqualToComparingFieldByField(originalEvent);
}

@Test
void eventReplay_shouldRespectOrderingGuarantees() {
    EventStreamExport export = loadEvents("concurrent-updates.json");
    
    // When: Replay
    replayService.replay(export);
    
    // Then: Final state matches production
    Event event = eventRepository.findById(export.getEventId()).get();
    assertThat(event.getVersion()).isEqualTo(export.getFinalVersion());
}
```

### ⚠️ Pułapki
1. **External Dependencies:** Email/SMS będą wysłane ponownie podczas replay!
   - **Rozwiązanie:** Mock external services w dev
2. **Non-Deterministic Code:** `UUID.randomUUID()`, `Instant.now()` dadzą inne wyniki
   - **Rozwiązanie:** Inject TimeProvider, UUIDProvider
3. **Data Privacy:** Production events mogą zawierać wrażliwe dane
   - **Rozwiązanie:** Anonymizacja przed exportem

---

## Temat 26: Event Versioning - Ewolucja Struktury

### Definicja
**Event Versioning** to techniki pozwalające zmieniać strukturę events bez breaking changes.

### Problem
```java
// V1: Event w produkcji (2024)
record EventCreatedEvent(UUID eventId, String name, String location) {}

// V2: Chcemy dodać nowe pole (2025)
record EventCreatedEvent(
    UUID eventId, 
    String name, 
    Location location,  // ← Changed: String → Object
    Integer capacity    // ← New field
) {}

// Catastrophe! Stare eventy w Event Store nie mają tych pól! 💥
```

### Rozwiązania

#### **Podejście 1: Weak Schema (Lazy Deserialization)**
```java
// Event Store: Zapisz jako JSON (bez strict validation)
{
  "type": "EventCreatedEvent",
  "version": 1,
  "data": {
    "eventId": "uuid",
    "name": "JavaConf",
    "location": "Warsaw"  // String
  }
}

// Deserializer: Obsługuje obie wersje
public class EventCreatedEvent {
    private UUID eventId;
    private String name;
    private Location location;  // Object
    private Integer capacity;   // Nullable!
    
    @JsonCreator
    public EventCreatedEvent(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("name") String name,
        @JsonProperty("location") Object locationRaw,  // Either String or Object
        @JsonProperty("capacity") Integer capacity
    ) {
        this.eventId = eventId;
        this.name = name;
        
        // Handle both formats
        if (locationRaw instanceof String) {
            this.location = Location.parse((String) locationRaw);  // V1
        } else {
            this.location = objectMapper.convertValue(locationRaw, Location.class);  // V2
        }
        
        this.capacity = capacity != null ? capacity : 100;  // Default for V1
    }
}
```

#### **Podejście 2: Explicit Versioning**
```java
// V1
record EventCreatedEventV1(UUID eventId, String name, String location) {}

// V2
record EventCreatedEventV2(
    UUID eventId, String name, Location location, Integer capacity
) {}

// Event Store
{
  "type": "EventCreatedEvent",
  "schemaVersion": 1,  // ← Explicit version
  "data": { ... }
}

// Deserializer
public DomainEvent deserialize(String json) {
    JsonNode root = objectMapper.readTree(json);
    int version = root.get("schemaVersion").asInt();
    
    return switch (version) {
        case 1 -> objectMapper.readValue(json, EventCreatedEventV1.class);
        case 2 -> objectMapper.readValue(json, EventCreatedEventV2.class);
        default -> throw new UnsupportedEventVersionException(version);
    };
}
```

#### **Podejście 3: Upcasting (Migration on Read)**
```java
// Upcaster: V1 → V2
public class EventCreatedEventUpcaster {
    
    public EventCreatedEventV2 upcast(EventCreatedEventV1 oldEvent) {
        return new EventCreatedEventV2(
            oldEvent.eventId(),
            oldEvent.name(),
            Location.parse(oldEvent.location()),  // String → Location
            100  // Default capacity for old events
        );
    }
}

// Event Store Reader
public DomainEvent read(UUID id) {
    StoredEvent stored = database.findById(id);
    
    DomainEvent event = deserialize(stored.getData());
    
    // Upcast if needed
    if (event instanceof EventCreatedEventV1 v1) {
        event = upcaster.upcast(v1);
    }
    
    return event;
}
```

### W EventMaster - Implementacja
```java
@Service
public class EventVersioningService {
    
    private final Map<Class<?>, List<Upcaster>> upcasters = new HashMap<>();
    
    public EventVersioningService() {
        registerUpcaster(
            EventCreatedEventV1.class,
            EventCreatedEventV2.class,
            this::upcastV1toV2
        );
    }
    
    private EventCreatedEventV2 upcastV1toV2(EventCreatedEventV1 v1) {
        return new EventCreatedEventV2(
            v1.eventId(),
            v1.name(),
            parseLocation(v1.location()),
            inferCapacity(v1.location())  // Smart default
        );
    }
    
    private Integer inferCapacity(String location) {
        // Business logic: Guess capacity from location
        if (location.contains("Stadium")) return 50000;
        if (location.contains("Hall")) return 500;
        return 100;
    }
}
```

### Testowanie
```java
@Test
void eventVersioning_shouldReadOldEvents() {
    // Given: Old V1 event in database
    String v1Json = """
        {
          "type": "EventCreatedEvent",
          "schemaVersion": 1,
          "data": {
            "eventId": "uuid-123",
            "name": "JavaConf",
            "location": "Warsaw, Poland"
          }
        }
        """;
    eventStore.saveRaw(v1Json);
    
    // When: Read (should auto-upcast)
    DomainEvent event = eventStore.read("uuid-123");
    
    // Then: It's V2 now
    assertThat(event).isInstanceOf(EventCreatedEventV2.class);
    EventCreatedEventV2 v2 = (EventCreatedEventV2) event;
    assertThat(v2.location().getCity()).isEqualTo("Warsaw");
    assertThat(v2.capacity()).isEqualTo(100);  // Default
}

@Test
void eventVersioning_shouldHandleMixedVersions() {
    // Given: Event with 3 versions in history
    UUID eventId = UUID.randomUUID();
    eventStore.append(eventId, new EventCreatedEventV1(...));  // 2024
    eventStore.append(eventId, new EventPublishedEventV1(...)); // 2024
    eventStore.append(eventId, new EventRenamedEventV2(...));  // 2025 - new version!
    
    // When: Reconstruct
    Event event = eventSourcingRepo.reconstruct(eventId);
    
    // Then: All versions processed correctly
    assertThat(event.getName()).isNotNull();
    assertThat(event.getStatus()).isEqualTo(PUBLISHED);
}
```

### Best Practices
1. **Additive Changes Only:** Nowe pola powinny być optional/nullable
2. **Never Remove Fields:** Mark as `@Deprecated`, ale nie usuwaj
3. **Version Everything:** Każdy event powinien mieć `schemaVersion`
4. **Test Upcasting:** ZAWSZE testuj migration path

### PostgreSQL 18 - Schema Evolution
```sql
-- Event Store z wersjonowaniem
CREATE TABLE event_store (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,  -- ← Version tracking
    event_data JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    -- Metadata for migration tracking
    migrated_from_version INTEGER,
    migrated_at TIMESTAMPTZ
);

-- Batch migration script
UPDATE event_store
SET 
    event_data = jsonb_set(
        event_data,
        '{capacity}',
        '100'  -- Add default capacity to V1 events
    ),
    schema_version = 2,
    migrated_from_version = 1,
    migrated_at = NOW()
WHERE event_type = 'EventCreatedEvent'
  AND schema_version = 1;
```

---

## Temat 27: Upcasting - Migracja Starych Events

### Definicja
**Upcasting** to proces transformacji starych events do nowej wersji podczas odczytu (lazy migration).

### Kiedy Używać?
- Miliony starych events w bazie (batch migration niemożliwa)
- Chcesz deployment bez downtime
- Event structure zmienia się często

### W EventMaster
```java
@Component
public class EventUpcasterChain {
    
    private final List<Upcaster<?, ?>> upcasters;
    
    public DomainEvent upcast(DomainEvent event) {
        DomainEvent current = event;
        
        for (Upcaster upcaster : upcasters) {
            if (upcaster.supports(current.getClass())) {
                current = upcaster.upcast(current);
            }
        }
        
        return current;
    }
}

// Konkretny Upcaster
@Component
public class EventCreatedV1toV2Upcaster implements Upcaster<EventCreatedEventV1, EventCreatedEventV2> {
    
    @Override
    public boolean supports(Class<?> eventClass) {
        return EventCreatedEventV1.class.equals(eventClass);
    }
    
    @Override
    public EventCreatedEventV2 upcast(EventCreatedEventV1 v1) {
        return new EventCreatedEventV2(
            v1.eventId(),
            v1.name(),
            Location.parse(v1.location()),
            100,  // Default
            Instant.now()  // Add new timestamp field
        );
    }
}
```

### Testowanie
```java
@Test
void upcaster_shouldTransformV1toV2() {
    EventCreatedEventV1 v1 = new EventCreatedEventV1(
        UUID.randomUUID(), "JavaConf", "Warsaw"
    );
    
    EventCreatedEventV2 v2 = upcaster.upcast(v1);
    
    assertThat(v2.eventId()).isEqualTo(v1.eventId());
    assertThat(v2.name()).isEqualTo(v1.name());
    assertThat(v2.location().toString()).isEqualTo("Warsaw");
    assertThat(v2.capacity()).isEqualTo(100);
}

@Test
void upcasterChain_shouldHandleMultipleVersions() {
    // V1 → V2 → V3
    EventCreatedEventV1 v1 = new EventCreatedEventV1(...);
    
    DomainEvent result = upcasterChain.upcast(v1);
    
    assertThat(result).isInstanceOf(EventCreatedEventV3.class);  // Latest!
}
```

---

## Temat 28: Event Compaction - Usuwanie Nieistotnych Events

### Definicja
**Event Compaction** to proces usuwania lub łączenia events, które nie wpływają na obecny stan.

### Przykład
```java
// PRZED compaction (10 events):
EventCreatedEvent(name: "Conf")
EventRenamedEvent(name: "Conf 2024")
EventRenamedEvent(name: "Conf 2025")
EventRenamedEvent(name: "JavaConf 2025")
EventRenamedEvent(name: "Ultimate JavaConf 2025")
... 5 more renames ...

// PO compaction (2 events):
EventCreatedEvent(name: "Conf")
EventRenamedEvent(name: "Ultimate JavaConf 2025")  // Ostatnia zmiana
```

### Testowanie
```java
@Test
void compaction_shouldRemoveIntermediateStates() {
    UUID eventId = UUID.randomUUID();
    
    // Given: 100 rename events
    for (int i = 0; i < 100; i++) {
        eventStore.append(eventId, new EventRenamedEvent(eventId, "Name " + i));
    }
    
    // When: Compact
    compactionService.compact(eventId);
    
    // Then: Only first and last
    List<DomainEvent> events = eventStore.getEvents(eventId);
    assertThat(events).hasSize(2);  // Create + Last Rename
}
```

---

## Temat 29: GDPR & Event Sourcing - "Prawo do Zapomnienia"

### Problem
GDPR Art. 17: "Prawo do usunięcia danych" (right to be forgotten)  
**ALE** Event Store jest append-only i immutable!

### Rozwiązania

#### **1. Crypto-Shredding**
```java
// Zapisz dane użytkownika zaszyfrowane
record UserCreatedEvent(
    UUID userId,
    String encryptedPersonalData,  // Encrypted with user-specific key
    String keyId
) {}

// "Zapomnienie" = usuń klucz szyfrowania
gdprService.forget(userId);  // Deletes encryption key
// Dane są w bazie, ale nie do odczytania!
```

#### **2. Tombstone Events**
```java
// Publikuj "UserForgottenEvent"
record UserForgottenEvent(UUID userId, Instant forgottenAt) {}

// Projector ignoruje wszystkie eventy tego usera
@KafkaListener
public void on(DomainEvent event) {
    if (isUserForgotten(event.getUserId())) {
        return;  // Skip
    }
    // ... normal processing
}
```

### Testowanie
```java
@Test
void gdpr_shouldMakeDataUnreadableAfterForgetting() {
    // Given
    UUID userId = UUID.randomUUID();
    eventStore.append(userId, new UserCreatedEvent(userId, encryptedData, keyId));
    
    // When: GDPR forget
    gdprService.forget(userId);
    
    // Then: Data is unreadable
    UserCreatedEvent event = eventStore.getEvents(userId).get(0);
    assertThatThrownBy(() -> decrypt(event.getEncryptedPersonalData()))
        .isInstanceOf(EncryptionKeyNotFoundException.class);
}
```

---

## Temat 30: Hybrid Approach - Best of Both Worlds

### Definicja
**Hybrid** = State-based (główna baza) + Event Sourcing (audit/history)

### W EventMaster - Rekomendacja
```java
// Write Model: Traditional state-based
@Entity
public class Event {
    @Id private UUID id;
    private String name;  // Current state
    // ... normal JPA
}

// Event Store: Append-only history
@Entity
public class EventHistory {
    @Id private UUID id;
    private UUID aggregateId;
    private String eventType;
    private String eventData;  // JSONB
}

// Na każdą zmianę:
public void updateEvent(UUID id, String newName) {
    // 1. Update main table (fast!)
    Event event = repository.findById(id).get();
    event.setName(newName);
    repository.save(event);
    
    // 2. Append to history (audit!)
    eventHistory.append(id, new EventRenamedEvent(id, newName));
}
```

### Zalety
✅ Fast reads (state-based)  
✅ Full history (event sourcing)  
✅ Easier to understand  
✅ GDPR-friendly (można usunąć state, zostawić events)

### Testowanie
```java
@Test
void hybrid_shouldMaintainBothStateAndHistory() {
    UUID id = createEvent("Initial");
    
    updateEvent(id, "Updated");
    
    // State: Latest
    Event event = repository.findById(id).get();
    assertThat(event.getName()).isEqualTo("Updated");
    
    // History: All changes
    List<DomainEvent> history = eventHistory.getEvents(id);
    assertThat(history).hasSize(2);  // Create + Rename
}
```

---

# CZĘŚĆ IV: MESSAGING & KAFKA (Tematy 31-40)

## Temat 31: Apache Kafka / Redpanda - Podstawy

### Definicja
**Kafka** to distributed streaming platform działający jak "dziennik" (log) dla events.

### Kluczowe Koncepcje
- **Topic** - kategoria messages (np. "events.lifecycle")
- **Partition** - podział topicu dla skalowalności
- **Offset** - pozycja w partition (0, 1, 2, ...)
- **Consumer Group** - grupa konsumentów dzielących pracę

### W EventMaster
```java
@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic eventsLifecycleTopic() {
        return TopicBuilder.name("events.lifecycle")
            .partitions(3)
            .replicas(1)
            .build();
    }
}
```

---

## Temat 32: Producer & Consumer Patterns

### Producer
```java
@Service
public class EventPublisher {
    @Autowired private KafkaTemplate<String, EventCreatedEvent> kafka;
    
    public void publish(EventCreatedEvent event) {
        kafka.send("events.lifecycle", event.getEventId().toString(), event);
    }
}
```

### Consumer
```java
@Service
public class EventConsumer {
    @KafkaListener(topics = "events.lifecycle", groupId = "projectors")
    public void consume(EventCreatedEvent event) {
        projector.handle(event);
    }
}
```

### Testowanie
```java
@Test
void kafka_shouldDeliverMessage() {
    EventCreatedEvent event = new EventCreatedEvent(...);
    
    publisher.publish(event);
    
    await().atMost(5, SECONDS).untilAsserted(() ->
        assertThat(consumer.getReceivedEvents()).contains(event)
    );
}
```

---

## Temat 33: Partitioning Strategy - Strategia Partycjonowania

### Definicja
**Partitioning** dzieli topic na części dla równoległego przetwarzania.

### Klucz Partycji
```java
// Events tego samego eventu trafiają do tej samej partycji
kafka.send("events.lifecycle", 
    event.getEventId().toString(),  // ← Partition key
    event
);

// Gwarantuje: kolejność dla tego samego eventu
```

### Testowanie
```java
@Test
void partitioning_shouldPreserveOrderForSameKey() {
    UUID eventId = UUID.randomUUID();
    
    publisher.publish(new EventCreatedEvent(eventId));
    publisher.publish(new EventPublishedEvent(eventId));
    publisher.publish(new EventCancelledEvent(eventId));
    
    await().untilAsserted(() -> {
        List<DomainEvent> received = consumer.getEventsFor(eventId);
        assertThat(received.get(0)).isInstanceOf(EventCreatedEvent.class);
        assertThat(received.get(1)).isInstanceOf(EventPublishedEvent.class);
        assertThat(received.get(2)).isInstanceOf(EventCancelledEvent.class);
    });
}
```

---

## Temat 34: Consumer Groups - Load Balancing

### Definicja
**Consumer Group** to grupa konsumentów dzielących pracę przy przetwarzaniu messages z tego samego topic.

### Analogia
Paczki na taśmie produkcyjnej:
- **Bez Consumer Group:** 1 pracownik musi przetworzyć wszystkie paczki (wolno! 🐌)
- **Z Consumer Group:** 5 pracowników dzieli paczki między siebie (szybko! 🚀)

### Jak To Działa?
```
Topic "events.lifecycle" ma 3 partycje:
Partition 0: [Event A, Event D, Event G]
Partition 1: [Event B, Event E, Event H]
Partition 2: [Event C, Event F, Event I]

Consumer Group "projectors" ma 3 konsumentów:
Consumer 1 → czyta Partition 0
Consumer 2 → czyta Partition 1
Consumer 3 → czyta Partition 2

Każdy event jest przetwarzany DOKŁADNIE RAZ przez 1 konsumenta!
```

### W EventMaster
```java
// Backend: Event Management Service
@Service
public class EventProjector {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "event-projectors",  // ← Consumer Group
        concurrency = "3"  // 3 konsumentów w tej grupie
    )
    public void projectEvent(EventCreatedEvent event) {
        log.info("Consumer {} processing event {}", 
            Thread.currentThread().getName(), event.getEventId());
        
        EventView view = new EventView(event);
        repository.save(view);
    }
}

// Inne serwisy mogą mieć swoje Consumer Groups
@Service
public class EmailNotificationService {
    
    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "email-notifications",  // ← Inna grupa
        concurrency = "5"
    )
    public void sendEmail(EventCreatedEvent event) {
        emailService.sendEventCreatedEmail(event);
    }
}

// Ten sam event trafia DO OBU GRUP!
// Ale w ramach grupy - tylko do 1 konsumenta
```

### Skalowanie
```bash
# Start z 1 instancją (1 consumer)
docker-compose up -d backend --scale backend=1

# Skaluj do 3 instancji (3 consumers w tej samej grupie)
docker-compose up -d backend --scale backend=3

# Kafka automatycznie rebalansuje partycje!
# Instance 1 → Partition 0
# Instance 2 → Partition 1
# Instance 3 → Partition 2
```

### Testowanie
```java
@SpringBootTest
@EmbeddedKafka(partitions = 3)
public class ConsumerGroupTest {
    
    @Test
    void consumerGroup_shouldDistributeLoad() throws Exception {
        CountDownLatch latch = new CountDownLatch(9);
        Set<String> consumedByThreads = ConcurrentHashMap.newKeySet();
        
        // Mock consumer tracking which thread processes which event
        doAnswer(inv -> {
            consumedByThreads.add(Thread.currentThread().getName());
            latch.countDown();
            return null;
        }).when(mockProjector).projectEvent(any());
        
        // When: Publish 9 events
        for (int i = 0; i < 9; i++) {
            kafkaTemplate.send("events.lifecycle", 
                "key-" + i, 
                new EventCreatedEvent(UUID.randomUUID(), "Event " + i)
            );
        }
        
        // Then: Wait for all to be consumed
        assertThat(latch.await(10, SECONDS)).isTrue();
        
        // Different threads processed events (load balanced!)
        assertThat(consumedByThreads).hasSizeGreaterThanOrEqualTo(2);
    }
    
    @Test
    void consumerGroup_shouldNotProcessSameEventTwice() {
        UUID eventId = UUID.randomUUID();
        Set<UUID> processedEvents = ConcurrentHashMap.newKeySet();
        
        // When: Publish 1 event
        kafkaTemplate.send("events.lifecycle", 
            eventId.toString(), 
            new EventCreatedEvent(eventId, "JavaConf")
        );
        
        // Then: Only 1 consumer processes it
        await().atMost(5, SECONDS).untilAsserted(() -> {
            verify(mockProjector, times(1)).projectEvent(any());
        });
    }
}
```

### Konfiguracja w EventMaster
```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: kafka:9092
    consumer:
      group-id: event-management-backend
      auto-offset-reset: earliest
      max-poll-records: 100  # Ile messages na raz
      properties:
        partition.assignment.strategy: 
          - org.apache.kafka.clients.consumer.RangeAssignor
          - org.apache.kafka.clients.consumer.RoundRobinAssignor
```

### ⚠️ Pułapki
1. **More consumers than partitions:** Nadmiarowe konsumenty będą idle!
   ```
   Topic ma 3 partycje
   Consumer Group ma 5 konsumentów
   → 3 konsumentów pracują, 2 idle 😴
   ```
2. **Rebalancing:** Gdy dodajesz/usuwasz konsumenta, Kafka rebalansuje (krótka przerwa)
3. **Same groupId = competition:** Różne serwisy powinny mieć różne groupId

---

## Temat 35: Offset Management - Śledzenie Postępu

### Definicja
**Offset** to pozycja w partition (0, 1, 2, ...). Consumer zapisuje "do którego offsetu doszedł".

### Analogia
Czytasz książkę:
- **Offset** = numer strony
- **Commit** = wkładasz zakładkę
- Gdy wrócisz, czytasz od zakładki (nie od początku!)

### Manual vs Auto Commit
```java
// ❌ AUTO COMMIT (default, ryzykowne)
@KafkaListener(topics = "events.lifecycle")
public void handle(EventCreatedEvent event) {
    processEvent(event);  // Co jeśli to rzuci exception?
    // Kafka auto-commituje offset PRZED przetworzeniem!
    // Jeśli fail → message LOST! 😱
}

// ✅ MANUAL COMMIT (safer)
@KafkaListener(topics = "events.lifecycle")
public void handle(
    EventCreatedEvent event,
    Acknowledgment ack  // ← Manual ack
) {
    try {
        processEvent(event);
        ack.acknowledge();  // Commit AFTER success
    } catch (Exception e) {
        log.error("Failed to process", e);
        // Nie commitujemy - Kafka redelivers
    }
}
```

### W EventMaster - Konfiguracja
```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false  # Manual control!
      properties:
        max.poll.interval.ms: 300000  # 5 minutes max processing time
```

```java
@Configuration
@EnableKafka
public class KafkaConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DomainEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DomainEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);  // ← Manual!
        
        return factory;
    }
}
```

### Testowanie
```java
@Test
void offset_shouldNotCommitOnFailure() {
    // Given: Consumer that fails
    doThrow(new RuntimeException("DB Error"))
        .when(mockRepository).save(any());
    
    UUID eventId = UUID.randomUUID();
    
    // When: Publish event
    kafkaTemplate.send("events.lifecycle", new EventCreatedEvent(eventId, "Test"));
    
    // Then: Consumer retries (offset not committed)
    await().atMost(10, SECONDS).untilAsserted(() -> {
        verify(mockRepository, atLeast(3)).save(any());  // Retried!
    });
}

@Test
void offset_shouldCommitOnSuccess() {
    // Given
    EventCreatedEvent event = new EventCreatedEvent(UUID.randomUUID(), "Test");
    
    // When
    kafkaTemplate.send("events.lifecycle", event);
    
    // Then: Processed once
    await().atMost(5, SECONDS).untilAsserted(() -> {
        verify(mockRepository, times(1)).save(any());
    });
    
    // And: Offset committed (no redelivery on restart)
    restartConsumer();
    Thread.sleep(2000);
    verify(mockRepository, times(1)).save(any());  // Still 1!
}
```

### Offset Storage w PostgreSQL 18
```sql
-- Kafka stores offsets internally, but you can track in DB too
CREATE TABLE kafka_consumer_offsets (
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INTEGER NOT NULL,
    offset_value BIGINT NOT NULL,
    committed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- Trigger to log offset commits
CREATE OR REPLACE FUNCTION log_offset_commit() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kafka_offset_log (consumer_group, topic, partition, offset_value)
    VALUES (NEW.consumer_group, NEW.topic, NEW.partition, NEW.offset_value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

---

## Temat 36: Dead Letter Queue (DLQ) - Obsługa Nieprzetwarzalnych Messages

### Definicja
**DLQ** to specjalny topic, do którego trafiają messages, których nie udało się przetworzyć po wielu próbach.

### Dlaczego Potrzebne?
```java
// Scenario: Poison pill message
{
  "eventId": "malformed-uuid",  // ← Invalid UUID!
  "name": null  // ← Null name!
}

// Consumer próbuje przetworzyć 1000x
// Blokuje całą partition! 😱
// Inne messages nie mogą być przetworzone!
```

### Rozwiązanie: DLQ
```
Message → Consumer → Success? → ACK
                   ↓
                  FAIL
                   ↓
              Retry 3x?
                   ↓
                  YES → Retry
                   ↓
                   NO
                   ↓
         Move to DLQ → ACK original message
                   ↓
         Manual investigation
```

### W EventMaster
```java
@Service
public class EventProjector {
    
    @KafkaListener(topics = "events.lifecycle", groupId = "projectors")
    @RetryableTopic(
        attempts = "3",  // Retry 3 times
        backoff = @Backoff(delay = 1000, multiplier = 2.0),  // 1s, 2s, 4s
        dltTopicSuffix = "-dlq",  // Dead Letter Queue topic
        include = {
            DataIntegrityViolationException.class,
            ValidationException.class
        }
    )
    public void projectEvent(EventCreatedEvent event) {
        // Walidacja
        if (event.getEventId() == null) {
            throw new ValidationException("EventId cannot be null");
        }
        
        // Save to DB
        EventView view = new EventView(event);
        repository.save(view);  // Może rzucić DataIntegrityViolationException
    }
    
    // DLQ Handler
    @KafkaListener(topics = "events.lifecycle-dlq", groupId = "dlq-handlers")
    public void handleDlqMessage(
        EventCreatedEvent event,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exception
    ) {
        log.error("Message moved to DLQ. Topic: {}, Exception: {}, Event: {}", 
            topic, exception, event);
        
        // Alert ops team
        alertService.sendSlackAlert(
            "DLQ Alert",
            String.format("Failed to process event %s. Reason: %s", 
                event.getEventId(), exception)
        );
        
        // Store for manual investigation
        dlqRepository.save(new DlqEntry(topic, event, exception, Instant.now()));
    }
}
```

### DLQ Table w PostgreSQL 18
```sql
CREATE TABLE dlq_messages (
    id BIGSERIAL PRIMARY KEY,
    original_topic VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    message_key VARCHAR(255),
    message_payload JSONB NOT NULL,
    exception_type VARCHAR(255) NOT NULL,
    exception_message TEXT,
    stack_trace TEXT,
    failed_at TIMESTAMPTZ DEFAULT NOW(),
    retry_count INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, INVESTIGATING, RESOLVED, DISCARDED
    resolved_at TIMESTAMPTZ,
    resolved_by VARCHAR(255)
);

CREATE INDEX idx_dlq_status ON dlq_messages (status, failed_at);
CREATE INDEX idx_dlq_topic ON dlq_messages (original_topic);
```

### Testowanie
```java
@Test
void dlq_shouldMovePoisonPillAfterRetries() {
    // Given: Message that always fails
    EventCreatedEvent poisonPill = new EventCreatedEvent(null, "Invalid");  // null ID!
    
    CountDownLatch dlqLatch = new CountDownLatch(1);
    doAnswer(inv -> {
        dlqLatch.countDown();
        return null;
    }).when(mockDlqHandler).handleDlqMessage(any(), anyString(), anyString());
    
    // When: Publish poison pill
    kafkaTemplate.send("events.lifecycle", poisonPill);
    
    // Then: After 3 retries, moves to DLQ
    assertThat(dlqLatch.await(15, SECONDS)).isTrue();
    
    // Verify retried 3 times before DLQ
    await().untilAsserted(() -> {
        verify(mockProjector, times(3)).projectEvent(poisonPill);
    });
    
    // Verify DLQ handler called once
    verify(mockDlqHandler, times(1)).handleDlqMessage(
        eq(poisonPill),
        contains("events.lifecycle"),
        contains("ValidationException")
    );
}

@Test
void dlq_shouldNotBlockOtherMessages() {
    // Given: 1 poison pill, 9 valid messages
    EventCreatedEvent poisonPill = new EventCreatedEvent(null, "Invalid");
    List<EventCreatedEvent> validEvents = createValidEvents(9);
    
    // When: Publish all
    kafkaTemplate.send("events.lifecycle", poisonPill);
    validEvents.forEach(e -> kafkaTemplate.send("events.lifecycle", e));
    
    // Then: Valid events processed successfully
    await().atMost(10, SECONDS).untilAsserted(() -> {
        assertThat(repository.count()).isEqualTo(9);
    });
    
    // Poison pill in DLQ
    assertThat(dlqRepository.count()).isEqualTo(1);
}
```

### DLQ Dashboard (Admin UI)
```java
@RestController
@RequestMapping("/api/v1/admin/dlq")
public class DlqAdminController {
    
    @GetMapping
    public Page<DlqEntry> listDlqMessages(
        @RequestParam(defaultValue = "PENDING") String status,
        Pageable pageable
    ) {
        return dlqRepository.findByStatus(status, pageable);
    }
    
    @PostMapping("/{id}/retry")
    public ResponseEntity<?> retryMessage(@PathVariable Long id) {
        DlqEntry entry = dlqRepository.findById(id)
            .orElseThrow(() -> new NotFoundException("DLQ entry not found"));
        
        // Republish to original topic
        kafkaTemplate.send(entry.getOriginalTopic(), entry.getMessagePayload());
        
        entry.setRetryCount(entry.getRetryCount() + 1);
        entry.setStatus("RETRYING");
        dlqRepository.save(entry);
        
        return ResponseEntity.ok("Message republished");
    }
    
    @PostMapping("/{id}/discard")
    public ResponseEntity<?> discardMessage(@PathVariable Long id) {
        DlqEntry entry = dlqRepository.findById(id)
            .orElseThrow(() -> new NotFoundException("DLQ entry not found"));
        
        entry.setStatus("DISCARDED");
        entry.setResolvedAt(Instant.now());
        dlqRepository.save(entry);
        
        return ResponseEntity.ok("Message discarded");
    }
}
```

---

## Temat 37: Exactly-Once Semantics - Gwarancja "Dokładnie Raz"

### Definicja
**Exactly-Once Semantics** gwarantuje, że message jest przetworzony **dokładnie raz**, nawet przy awariach.

### Porównanie
- **At-Most-Once:** Message może być lost (nie OK! 😞)
- **At-Least-Once:** Message może być delivered 2x (wymaga idempotency! ⚠️)
- **Exactly-Once:** Message delivered i processed dokładnie 1x (ideał! 🎉)

### Problem bez Exactly-Once
```java
// Scenario: Payment processing
@KafkaListener(topics = "payments")
public void processPayment(PaymentEvent payment) {
    // 1. Charge credit card
    stripeService.charge(payment.getAmount());  // ← SUCCESS
    
    // 2. Save to DB
    repository.save(payment);  // ← CRASH! 💥
    
    // Kafka nie dostał ACK → redelivery
    // Karta zostanie obciążona DRUGI RAZ! 😱
}
```

### Rozwiązanie 1: Idempotency (At-Least-Once + Deduplication)
```java
@KafkaListener(topics = "payments")
public void processPayment(PaymentEvent payment) {
    // Check if already processed
    if (repository.existsByPaymentId(payment.getPaymentId())) {
        log.warn("Payment {} already processed, skipping", payment.getPaymentId());
        return;  // Idempotent! ✅
    }
    
    stripeService.charge(payment.getAmount());
    repository.save(payment);
}
```

### Rozwiązanie 2: Exactly-Once z Kafka Transactions
```java
@Configuration
public class KafkaExactlyOnceConfig {
    
    @Bean
    public ProducerFactory<String, DomainEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "event-management-tx");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // ← Exactly-once
        
        return new DefaultKafkaProducerFactory<>(config);
    }
}

@Service
public class TransactionalEventService {
    
    @Autowired
    private KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    @Transactional  // DB transaction
    public void createEventTransactionally(CreateEventCommand cmd) {
        // 1. Start Kafka transaction
        kafkaTemplate.executeInTransaction(kt -> {
            // 2. Save to DB (PostgreSQL transaction)
            Event event = new Event(cmd);
            eventRepository.save(event);
            
            // 3. Publish to Kafka (same transaction!)
            kt.send("events.lifecycle", new EventCreatedEvent(event));
            
            return true;
        });
        
        // Both DB & Kafka commit together (atomic!)
        // If DB fails → Kafka rollback
        // If Kafka fails → DB rollback
    }
}
```

### W EventMaster - Transactional Outbox Pattern
```sql
-- Outbox table (PostgreSQL 18)
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'PENDING'
);

CREATE INDEX idx_outbox_pending ON outbox (status, created_at) 
WHERE status = 'PENDING';
```

```java
@Service
public class OutboxPublisher {
    
    @Transactional
    public void createEvent(CreateEventCommand cmd) {
        // 1. Save to DB
        Event event = new Event(cmd);
        eventRepository.save(event);
        
        // 2. Save to Outbox (same DB transaction!)
        OutboxEntry outbox = new OutboxEntry(
            event.getId(),
            "EventCreatedEvent",
            toJson(event),
            Instant.now()
        );
        outboxRepository.save(outbox);
        
        // Both commits together! Atomic! ✅
    }
    
    // Separate process: Outbox publisher
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEntry> pending = outboxRepository
            .findByStatusOrderByCreatedAt("PENDING", PageRequest.of(0, 100));
        
        for (OutboxEntry entry : pending) {
            try {
                // Publish to Kafka
                kafkaTemplate.send("events.lifecycle", 
                    entry.getAggregateId().toString(),
                    deserialize(entry.getEventPayload())
                );
                
                // Mark as processed
                entry.setStatus("PUBLISHED");
                entry.setProcessedAt(Instant.now());
                outboxRepository.save(entry);
                
            } catch (Exception e) {
                log.error("Failed to publish outbox entry {}", entry.getId(), e);
                // Will retry next iteration
            }
        }
    }
}
```

### Testowanie
```java
@Test
void exactlyOnce_shouldNotDuplicateOnRetry() {
    // Given: Payment event with unique ID
    UUID paymentId = UUID.randomUUID();
    PaymentEvent payment = new PaymentEvent(paymentId, Money.of(100, "PLN"));
    
    // When: Publish 3 times (simulate retry)
    for (int i = 0; i < 3; i++) {
        kafkaTemplate.send("payments", payment);
    }
    
    // Then: Processed only once
    await().atMost(10, SECONDS).untilAsserted(() -> {
        assertThat(paymentRepository.findAll()).hasSize(1);
        verify(stripeMock, times(1)).charge(any());  // Charged once!
    });
}

@Test
void transactionalOutbox_shouldNotLoseMessages() {
    // Given: 100 events
    for (int i = 0; i < 100; i++) {
        service.createEvent(new CreateEventCommand("Event " + i));
    }
    
    // Then: All in DB
    assertThat(eventRepository.count()).isEqualTo(100);
    
    // And: All in Outbox
    assertThat(outboxRepository.count()).isEqualTo(100);
    
    // When: Publisher runs
    outboxPublisher.publishPendingEvents();
    
    // Then: All published to Kafka
    await().atMost(15, SECONDS).untilAsserted(() -> {
        assertThat(outboxRepository.findByStatus("PUBLISHED")).hasSize(100);
    });
}
```

---

## Temat 38: Schema Registry - Wersjonowanie Struktury Messages

### Definicja
**Schema Registry** to centralna usługa przechowująca i weryfikująca strukturę messages (Avro, Protobuf, JSON Schema).

### Problem bez Schema Registry
```java
// Producer (Backend V1)
kafka.send("events", new EventCreatedEvent(UUID, String, String));

// Consumer (Analytics V2) expects:
EventCreatedEvent(UUID, String, Location, Integer)  // ← INCOMPATIBLE! 💥
```

### Rozwiązanie: Schema Registry
```yaml
# docker-compose.yml
services:
  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
    ports:
      - "8081:8081"
```

### Avro Schema dla EventCreatedEvent
```json
{
  "type": "record",
  "name": "EventCreatedEvent",
  "namespace": "com.eventmaster.events",
  "fields": [
    {"name": "eventId", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "capacity", "type": ["null", "int"], "default": null}
  ]
}
```

### W EventMaster
```java
@Configuration
public class SchemaRegistryConfig {
    
    @Bean
    public ProducerFactory<String, SpecificRecordBase> avroProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put("schema.registry.url", "http://schema-registry:8081");
        
        return new DefaultKafkaProducerFactory<>(config);
    }
}

// Producer
@Service
public class AvroEventPublisher {
    
    public void publish(Event event) {
        EventCreatedEvent avroEvent = EventCreatedEvent.newBuilder()
            .setEventId(event.getId().toString())
            .setName(event.getName())
            .setLocation(event.getLocation())
            .setCapacity(event.getCapacity())
            .build();
        
        kafkaTemplate.send("events.lifecycle.avro", avroEvent);
    }
}
```

### Testowanie
```java
@Test
void schemaRegistry_shouldValidateSchema() {
    // Given: Valid Avro event
    EventCreatedEvent validEvent = EventCreatedEvent.newBuilder()
        .setEventId(UUID.randomUUID().toString())
        .setName("JavaConf")
        .setLocation("Warsaw")
        .build();
    
    // When: Publish
    publisher.publish(validEvent);
    
    // Then: Success
    await().untilAsserted(() -> 
        assertThat(consumer.getReceivedEvents()).contains(validEvent)
    );
}

@Test
void schemaRegistry_shouldRejectIncompatibleSchema() {
    // Given: Incompatible schema (removed required field)
    // This would be caught at compile time with Avro!
    
    // But if sent as raw bytes:
    byte[] incompatibleData = createIncompatibleAvroBytes();
    
    // When: Try to deserialize
    assertThatThrownBy(() -> 
        avroDeserializer.deserialize("events.lifecycle.avro", incompatibleData)
    ).isInstanceOf(SerializationException.class);
}
```

---

## Temat 39: Kafka Streams - Przetwarzanie w Czasie Rzeczywistym

### Definicja
**Kafka Streams** to library do przetwarzania stream danych (filtering, aggregation, joins).

### W EventMaster - Przykład: Real-time Analytics
```java
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
    
    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }
    
    @Bean
    public KStream<String, EventCreatedEvent> eventStream(StreamsBuilder builder) {
        // Input stream
        KStream<String, EventCreatedEvent> events = 
            builder.stream("events.lifecycle");
        
        // Filter: Only published events
        KStream<String, EventCreatedEvent> publishedEvents = events
            .filter((key, event) -> event.getStatus() == Status.PUBLISHED);
        
        // Group by city and count
        KTable<String, Long> eventsByCity = publishedEvents
            .groupBy((key, event) -> event.getCity())
            .count(Materialized.as("events-by-city-store"));
        
        // Output to new topic
        eventsByCity.toStream()
            .to("analytics.events-by-city", Produced.with(Serdes.String(), Serdes.Long()));
        
        return events;
    }
}
```

### Testowanie
```java
@Test
void kafkaStreams_shouldAggregateEventsByCity() {
    // Given: 3 events in Warsaw, 2 in Krakow
    publishEvent("Warsaw");
    publishEvent("Warsaw");
    publishEvent("Warsaw");
    publishEvent("Krakow");
    publishEvent("Krakow");
    
    // When: Kafka Streams processes
    // Then: Analytics topic has aggregated data
    await().atMost(10, SECONDS).untilAsserted(() -> {
        Map<String, Long> counts = consumeAnalyticsTopic();
        assertThat(counts.get("Warsaw")).isEqualTo(3L);
        assertThat(counts.get("Krakow")).isEqualTo(2L);
    });
}
```

---

## Temat 40: Transactional Outbox - Gwarancja Publikacji

### Omówione w Temacie 37 - podsumowanie:

**Pattern:**
1. Save to DB + Outbox (single transaction)
2. Background job publishes from Outbox to Kafka
3. Guaranteed: If saved to DB → will be published

**Zalety:**
✅ No lost messages  
✅ Exactly-once semantics  
✅ Survives crashes  

**Implementacja w EventMaster:**
- PostgreSQL 18 Outbox table
- Spring @Scheduled publisher
- Idempotency w konsumentach

---

---

# CZĘŚĆ V: TESTOWANIE & OPERACJE (Tematy 41-50)

## Temat 41: Integration Testing with Testcontainers

### Definicja
**Testcontainers** to Java library pozwalająca uruchomić rzeczywiste kontenery Docker w testach integracyjnych.

### Dlaczego Testcontainers?
**Bez Testcontainers:**
```java
// ❌ In-memory H2 Database
@SpringBootTest
public class EventTest {
    // H2 ≠ PostgreSQL!
    // H2 nie ma JSONB, advanced indexes, etc.
    // Test PASSED, production FAILED! 😱
}
```

**Z Testcontainers:**
```java
// ✅ Real PostgreSQL 18 + Apache Kafka
@SpringBootTest
public class EventTest {
    @Container
    static PostgreSQLContainer<?> postgres = 
        new PostgreSQLContainer<>("postgres:18-alpine");
    
    @Container
    static KafkaContainer kafka = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
    
    // Test używa PRAWDZIWEJ bazy! ✅
}
```

### W EventMaster - Complete Setup
```java
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Testcontainers
public abstract class BaseIntegrationTest {
    
    // PostgreSQL 18
    @Container
    static PostgreSQLContainer<?> postgresContainer = 
        new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("eventmaster_test")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("db/init-test-data.sql");  // Optional: Seed data
    
    // Apache Kafka (compatible API)
    @Container
    static KafkaContainer kafkaContainer = 
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withExposedPorts(9093);
    
    // Redis (dla cache)
    @Container
    static GenericContainer<?> redisContainer = 
        new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        // PostgreSQL
        registry.add("spring.datasource.url", postgresContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgresContainer::getUsername);
        registry.add("spring.datasource.password", postgresContainer::getPassword);
        
        // Kafka
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        
        // Redis
        registry.add("spring.redis.host", redisContainer::getHost);
        registry.add("spring.redis.port", () -> 
            redisContainer.getMappedPort(6379).toString()
        );
    }
    
    @Autowired
    protected TestRestTemplate restTemplate;
    
    @Autowired
    protected EventRepository eventRepository;
    
    @Autowired
    protected KafkaTemplate<String, DomainEvent> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        // Clean before each test
        eventRepository.deleteAll();
    }
}
```

### Przykładowe Testy
```java
public class EventManagementIntegrationTest extends BaseIntegrationTest {
    
    @Test
    void shouldCreateEventEndToEnd() {
        // Given
        CreateEventDTO dto = new CreateEventDTO(
            "JavaConf 2025",
            "Amazing conference",
            "Warsaw, Poland",
            LocalDateTime.now().plusMonths(1)
        );
        
        // When: HTTP POST
        ResponseEntity<EventDTO> response = restTemplate.postForEntity(
            "/api/v1/events",
            dto,
            EventDTO.class
        );
        
        // Then: HTTP 201
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody().getName()).isEqualTo("JavaConf 2025");
        
        // And: Saved to PostgreSQL
        UUID eventId = response.getBody().getId();
        Event savedEvent = eventRepository.findById(eventId).orElseThrow();
        assertThat(savedEvent.getName()).isEqualTo("JavaConf 2025");
        
        // And: Event published to Kafka
        await().atMost(5, SECONDS).untilAsserted(() -> {
            List<EventCreatedEvent> events = consumeKafka("events.lifecycle");
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getEventId()).isEqualTo(eventId);
        });
    }
    
    @Test
    void shouldHandlePostgreSQLUniqueConstraint() {
        // Given: Event już istnieje
        Event existingEvent = new Event(UUID.randomUUID(), "JavaConf", ...);
        eventRepository.save(existingEvent);
        
        // When: Próba utworzenia duplikatu (same name + date)
        CreateEventDTO dto = new CreateEventDTO("JavaConf", ...);
        
        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
            "/api/v1/events",
            dto,
            ErrorResponse.class
        );
        
        // Then: HTTP 409 Conflict
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().getMessage())
            .contains("Event with this name and date already exists");
    }
    
    @Test
    void shouldHandleKafkaUnavailability() {
        // Given: Kafka stopped
        kafkaContainer.stop();
        
        try {
            // When: Try to create event
            CreateEventDTO dto = new CreateEventDTO("JavaConf", ...);
            
            ResponseEntity<EventDTO> response = restTemplate.postForEntity(
                "/api/v1/events",
                dto,
                EventDTO.class
            );
            
            // Then: Still succeeds (Outbox pattern!)
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            
            // Event w bazie
            assertThat(eventRepository.count()).isEqualTo(1);
            
            // Event w Outbox (czeka na publikację)
            assertThat(outboxRepository.findByStatus("PENDING")).hasSize(1);
            
        } finally {
            kafkaContainer.start();  // Restart for other tests
        }
    }
}
```

### Advanced: Reusable Containers (Faster Tests)
```java
// Kontenery startują RAZ dla wszystkich testów (nie za każdym razem)
@SpringBootTest
@Testcontainers
public abstract class BaseFastIntegrationTest {
    
    private static final PostgreSQLContainer<?> postgresContainer;
    private static final KafkaContainer kafkaContainer;
    
    static {
        // Start ONCE
        postgresContainer = new PostgreSQLContainer<>("postgres:18-alpine")
            .withReuse(true);  // ← Reuse!
        postgresContainer.start();
        
        kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
        ).withReuse(true);
        kafkaContainer.start();
    }
    
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgresContainer::getJdbcUrl);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }
}

// Benchmark:
// Without reuse: 100 tests = 15 minutes (starting containers 100x)
// With reuse:    100 tests = 2 minutes (starting containers 1x) 🚀
```

### Testowanie z Docker Compose
```java
@SpringBootTest
@Testcontainers
public class DockerComposeIntegrationTest {
    
    @Container
    static DockerComposeContainer<?> compose = 
        new DockerComposeContainer<>(new File("docker-compose.test.yml"))
            .withExposedService("postgres", 5432)
            .withExposedService("kafka", 9092)
            .withExposedService("redis", 6379);
    
    // Używa całego EventMaster stack!
}
```

### ⚠️ Pułapki
1. **Slow Tests:** Testcontainers dodaje 5-10s overhead na start
   - **Rozwiązanie:** Reusable containers
2. **CI/CD:** Jenkins/GitLab CI musi mieć Docker-in-Docker
3. **Port Conflicts:** Używaj `@Container` z random ports

---

## Temat 42: Testing Asynchronous Flows - Awaitility

### Definicja
**Awaitility** to DSL (Domain Specific Language) do testowania asynchronicznych operacji.

### Problem
```java
// ❌ BAD: Flaky test
@Test
void asyncTest() {
    publisher.publish(event);
    
    Thread.sleep(1000);  // 😱 Arbitrary delay!
    // Co jeśli Kafka jest wolny? Fail!
    // Co jeśli jest szybki? Marnujemy czas!
    
    assertThat(repository.findAll()).hasSize(1);
}
```

### Rozwiązanie: Awaitility
```java
// ✅ GOOD: Smart waiting
@Test
void asyncTest() {
    publisher.publish(event);
    
    await()
        .atMost(5, SECONDS)              // Max timeout
        .pollInterval(100, MILLISECONDS) // Check every 100ms
        .untilAsserted(() -> 
            assertThat(repository.findAll()).hasSize(1)
        );
}
```

### W EventMaster - Complete Examples
```java
public class EventProjectorTest extends BaseIntegrationTest {
    
    @Test
    void projector_shouldUpdateReadModel() {
        // Given
        UUID eventId = UUID.randomUUID();
        EventCreatedEvent event = new EventCreatedEvent(eventId, "JavaConf", ...);
        
        // When: Publish to Kafka
        kafkaTemplate.send("events.lifecycle", event);
        
        // Then: Wait for projector to process
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                Optional<EventView> view = eventViewRepository.findById(eventId);
                assertThat(view).isPresent();
                assertThat(view.get().getName()).isEqualTo("JavaConf");
                assertThat(view.get().getStatus()).isEqualTo("DRAFT");
            });
    }
    
    @Test
    void projector_shouldHandleMultipleEvents() {
        // Given: Sequence of events
        UUID eventId = UUID.randomUUID();
        
        // When: Publish sequence
        kafkaTemplate.send("events.lifecycle", 
            new EventCreatedEvent(eventId, "JavaConf", ...));
        kafkaTemplate.send("events.lifecycle", 
            new EventPublishedEvent(eventId, Instant.now()));
        kafkaTemplate.send("events.lifecycle", 
            new EventCancelledEvent(eventId, "Venue issue"));
        
        // Then: Final state is CANCELLED
        await()
            .atMost(10, SECONDS)
            .pollDelay(100, MILLISECONDS)  // Wait 100ms before first check
            .untilAsserted(() -> {
                EventView view = eventViewRepository.findById(eventId).orElseThrow();
                assertThat(view.getStatus()).isEqualTo("CANCELLED");
            });
    }
    
    @Test
    void projector_shouldHandleHighThroughput() {
        // Given: 1000 events
        List<UUID> eventIds = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            UUID id = UUID.randomUUID();
            eventIds.add(id);
            kafkaTemplate.send("events.lifecycle", 
                new EventCreatedEvent(id, "Event " + i, ...));
        }
        
        // Then: All processed within 30 seconds
        await()
            .atMost(30, SECONDS)
            .untilAsserted(() -> 
                assertThat(eventViewRepository.count()).isEqualTo(1000)
            );
        
        // And: All events present
        eventIds.forEach(id -> 
            assertThat(eventViewRepository.findById(id)).isPresent()
        );
    }
    
    @Test
    void projector_shouldRetryOnTransientFailure() {
        // Given: DB connection fails temporarily
        CountDownLatch failureCount = new CountDownLatch(2);
        
        doAnswer(inv -> {
            if (failureCount.getCount() > 0) {
                failureCount.countDown();
                throw new TransientDataAccessException("DB connection lost");
            }
            return inv.callRealMethod();
        }).when(spyRepository).save(any());
        
        // When: Publish event
        UUID eventId = UUID.randomUUID();
        kafkaTemplate.send("events.lifecycle", 
            new EventCreatedEvent(eventId, "JavaConf", ...));
        
        // Then: Eventually succeeds after retries
        await()
            .atMost(15, SECONDS)
            .untilAsserted(() -> {
                assertThat(eventViewRepository.findById(eventId)).isPresent();
                // Verify retried (failed 2x, succeeded 3rd time)
                verify(spyRepository, times(3)).save(any());
            });
    }
}
```

### Advanced Awaitility Patterns
```java
// Pattern 1: Condition with custom message
await()
    .atMost(5, SECONDS)
    .until(() -> repository.count() > 0, 
        is(true), 
        "Expected at least 1 event in repository");

// Pattern 2: Ignore exceptions during polling
await()
    .atMost(10, SECONDS)
    .ignoreExceptions()  // Ignore EntityNotFoundException
    .untilAsserted(() -> {
        Event event = repository.findById(id).orElseThrow();
        assertThat(event.getStatus()).isEqualTo(PUBLISHED);
    });

// Pattern 3: Custom polling strategy
await()
    .atMost(30, SECONDS)
    .pollDelay(1, SECONDS)           // Wait 1s before first check
    .pollInterval(500, MILLISECONDS) // Then check every 500ms
    .untilAsserted(() -> 
        assertThat(outboxRepository.findByStatus("PUBLISHED")).hasSize(100)
    );

// Pattern 4: Await multiple conditions
await()
    .atMost(10, SECONDS)
    .until(() -> 
        repository.count() == 10 && 
        eventViewRepository.count() == 10 &&
        outboxRepository.findByStatus("PENDING").isEmpty()
    );

// Pattern 5: Callable with return value
Long eventCount = await()
    .atMost(5, SECONDS)
    .until(() -> repository.count(), greaterThan(0L));
assertThat(eventCount).isGreaterThan(0);
```

### Base Test Class z Awaitility Utilities
```java
public abstract class BaseAsyncTest extends BaseIntegrationTest {
    
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    
    protected void awaitEvent(UUID eventId) {
        await()
            .atMost(DEFAULT_TIMEOUT)
            .pollInterval(POLL_INTERVAL)
            .untilAsserted(() -> 
                assertThat(eventRepository.findById(eventId)).isPresent()
            );
    }
    
    protected void awaitEventView(UUID eventId, String expectedStatus) {
        await()
            .atMost(DEFAULT_TIMEOUT)
            .pollInterval(POLL_INTERVAL)
            .untilAsserted(() -> {
                EventView view = eventViewRepository.findById(eventId).orElseThrow();
                assertThat(view.getStatus()).isEqualTo(expectedStatus);
            });
    }
    
    protected void awaitKafkaMessage(String topic, Predicate<DomainEvent> matcher) {
        await()
            .atMost(DEFAULT_TIMEOUT)
            .pollInterval(POLL_INTERVAL)
            .untilAsserted(() -> {
                List<DomainEvent> events = consumeKafka(topic);
                assertThat(events).anyMatch(matcher);
            });
    }
    
    protected void awaitOutboxEmpty() {
        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> 
                assertThat(outboxRepository.findByStatus("PENDING")).isEmpty()
            );
    }
}

// Usage:
@Test
void test() {
    publisher.publish(event);
    awaitEventView(event.getEventId(), "PUBLISHED");  // ✅ Clean!
}
```

### Performance Testing z Awaitility
```java
@Test
void performance_shouldProcessEventsWithinSLA() {
    // SLA: 95% of events processed within 2 seconds
    List<Long> processingTimes = new ArrayList<>();
    
    for (int i = 0; i < 100; i++) {
        UUID eventId = UUID.randomUUID();
        Instant start = Instant.now();
        
        kafkaTemplate.send("events.lifecycle", 
            new EventCreatedEvent(eventId, "Event " + i, ...));
        
        await()
            .atMost(10, SECONDS)
            .until(() -> eventViewRepository.findById(eventId).isPresent());
        
        Instant end = Instant.now();
        processingTimes.add(Duration.between(start, end).toMillis());
    }
    
    // Calculate 95th percentile
    Collections.sort(processingTimes);
    long p95 = processingTimes.get(94);  // 95th element (0-indexed)
    
    assertThat(p95).isLessThan(2000);  // Under 2 seconds
    log.info("P95 processing time: {}ms", p95);
}
```

### ⚠️ Pułapki
1. **Too Long Timeout:** `atMost(1, HOURS)` - test nigdy nie failuje w rozsądnym czasie
2. **Too Short Timeout:** `atMost(10, MILLISECONDS)` - false failures
3. **Ignoring Exceptions Blindly:** `ignoreExceptions()` może ukryć prawdziwe błędy
4. **Polling Too Often:** `pollInterval(1, MILLISECONDS)` - marnuje CPU

---

## Temat 43: Contract Testing - Testy Kontraktów

### Definicja
**Contract Testing** weryfikuje, czy Producer i Consumer zgadzają się co do formatu messages, zapobiegając breaking changes.

### Problem bez Contract Tests
```
Backend (Producer) zmienia EventCreatedEvent:
V1: { "eventId": "uuid", "name": "string" }
V2: { "id": "uuid", "eventName": "string" }  // ← Changed fields!

Analytics (Consumer) nadal oczekuje V1
→ PRODUCTION CRASH! 💥
```

### Rozwiązanie: Consumer-Driven Contracts
**Consumer** definiuje kontrakt (co oczekuje), **Producer** musi go spełnić.

### W EventMaster - Pact Framework

#### **Consumer Side (Analytics Service)**
```java
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "event-management-service", port = "8080")
public class EventAnalyticsConsumerPactTest {
    
    @Pact(consumer = "analytics-service")
    public RequestResponsePact createEventCreatedPact(PactDslWithProvider builder) {
        return builder
            .given("event with id 123e4567-e89b-12d3-a456-426614174000 exists")
            .uponReceiving("a request to get event")
                .path("/api/v1/events/123e4567-e89b-12d3-a456-426614174000")
                .method("GET")
            .willRespondWith()
                .status(200)
                .body(new PactDslJsonBody()
                    .uuid("eventId", "123e4567-e89b-12d3-a456-426614174000")  // ← Contract!
                    .stringType("name", "JavaConf 2025")
                    .stringType("location", "Warsaw, Poland")
                    .datetime("startDate", "yyyy-MM-dd'T'HH:mm:ss", 
                        LocalDateTime.now().plusMonths(1))
                    .numberType("capacity", 500)
                )
            .toPact();
    }
    
    @Test
    @PactTestFor(pactMethod = "createEventCreatedPact")
    void testEventAnalytics(MockServer mockServer) {
        // Given: Analytics service pointing to mock server
        AnalyticsClient client = new AnalyticsClient(mockServer.getUrl());
        
        // When: Fetch event
        EventDTO event = client.getEvent(
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
        );
        
        // Then: Response matches contract
        assertThat(event.getEventId()).isNotNull();
        assertThat(event.getName()).isEqualTo("JavaConf 2025");
    }
}
```

#### **Producer Side (Event Management Service)**
```java
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Provider("event-management-service")
@PactBroker(url = "http://pact-broker:9292")
public class EventManagementProviderPactTest {
    
    @LocalServerPort
    private int port;
    
    @Autowired
    private EventRepository eventRepository;
    
    @BeforeEach
    void setUp(PactVerificationContext context) {
        context.setTarget(new HttpTestTarget("localhost", port));
    }
    
    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void pactVerificationTestTemplate(PactVerificationContext context) {
        context.verifyInteraction();
    }
    
    @State("event with id 123e4567-e89b-12d3-a456-426614174000 exists")
    void eventExists() {
        // Setup state: Create event in DB
        Event event = new Event(
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
            "JavaConf 2025",
            "Warsaw, Poland",
            LocalDateTime.now().plusMonths(1),
            500
        );
        eventRepository.save(event);
    }
}
```

### Message Pact (Kafka)
```java
// Consumer: Analytics Service expects EventCreatedEvent
@ExtendWith(PactConsumerTestExt.class)
public class EventCreatedMessagePactTest {
    
    @Pact(consumer = "analytics-service", provider = "event-management-service")
    public MessagePact createEventCreatedMessagePact(MessagePactBuilder builder) {
        return builder
            .expectsToReceive("an EventCreatedEvent")
            .withContent(new PactDslJsonBody()
                .uuid("eventId")
                .stringType("name", "JavaConf 2025")
                .stringType("location", "Warsaw, Poland")
                .integerType("capacity", 500)
                .datetime("occurredAt", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            )
            .toPact();
    }
    
    @Test
    @PactTestFor(pactMethod = "createEventCreatedMessagePact")
    void testEventCreatedMessage(List<Message> messages) {
        // Given
        Message message = messages.get(0);
        
        // When: Deserialize
        EventCreatedEvent event = objectMapper.readValue(
            message.getContents().valueAsString(),
            EventCreatedEvent.class
        );
        
        // Then: Consumer can process it
        analyticsService.processEvent(event);
        assertThat(analyticsRepository.findAll()).hasSize(1);
    }
}

// Producer: Event Management verifies it publishes correct format
@SpringBootTest
@Provider("event-management-service")
@PactBroker
public class EventManagementMessageProviderTest {
    
    @Autowired
    private EventCommandHandler commandHandler;
    
    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void pactVerificationTestTemplate(PactVerificationContext context) {
        context.verifyInteraction();
    }
    
    @PactVerifyProvider("an EventCreatedEvent")
    public String verifyEventCreatedEvent() {
        // Produce event
        CreateEventCommand cmd = new CreateEventCommand(
            UUID.randomUUID(),
            "JavaConf 2025",
            "Warsaw, Poland",
            LocalDateTime.now().plusMonths(1),
            500
        );
        
        EventCreatedEvent event = commandHandler.handle(cmd);
        
        // Return JSON for verification
        return objectMapper.writeValueAsString(event);
    }
}
```

### Pact Broker Workflow
```yaml
# docker-compose.yml
services:
  pact-broker:
    image: pactfoundation/pact-broker:2.107.1
    ports:
      - "9292:9292"
    environment:
      PACT_BROKER_DATABASE_URL: postgresql://postgres:password@postgres:5432/pact_broker
      PACT_BROKER_BASIC_AUTH_USERNAME: pact
      PACT_BROKER_BASIC_AUTH_PASSWORD: pact
    depends_on:
      - postgres
```

**Workflow:**
1. Consumer writes contract → publishes to Pact Broker
2. Producer runs tests → verifies against contracts in Pact Broker
3. CI/CD: Can-I-Deploy check before deployment
   ```bash
   pact-broker can-i-deploy \
     --pacticipant event-management-service \
     --version 1.2.3 \
     --to production
   ```

### Testowanie
```java
@Test
void contractBreaking_shouldFailProviderTest() {
    // Given: Consumer expects "eventId" field
    // When: Producer changes to "id" field (breaking change!)
    
    // Then: Pact verification FAILS
    assertThatThrownBy(() -> 
        pactVerifier.verify()
    ).isInstanceOf(PactVerificationException.class)
     .hasMessageContaining("eventId: Expected but was missing");
}

@Test
void contractEvolution_shouldSupportBackwardCompatibility() {
    // Given: Consumer expects V1 (eventId, name)
    // When: Producer adds NEW field "capacity" (non-breaking)
    
    // Then: Pact verification PASSES
    pactVerifier.verify();  // ✅ Backward compatible
}
```

### Best Practices
1. **Consumer-Driven:** Consumer definiuje, Producer spełnia
2. **Version in Broker:** Zawsze taguj wersje (v1.0.0, v1.1.0)
3. **Can-I-Deploy:** Zawsze sprawdzaj przed deploymentem
4. **Additive Changes Only:** Nowe pola OK, usunięcie = breaking

---

## Temat 44: Monitoring & Observability - Obserwowanie Systemu

### Definicja
**Observability** to zdolność systemu do odpowiadania na pytania: "Co się stało?", "Dlaczego?" i "Jak to naprawić?"

### 3 Filary Observability
1. **Metrics** - liczby (CPU, memory, throughput, latency)
2. **Logs** - events (co się wydarzyło)
3. **Traces** - flow (jak request przechodzi przez system)

### W EventMaster - Complete Stack

#### **1. Metrics (Prometheus + Grafana)**

```yaml
# docker-compose.yml
services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    depends_on:
      - prometheus
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'eventmaster-backend'
    scrape_interval: 15s
    static_configs:
      - targets: ['backend:8080']
    metrics_path: '/actuator/prometheus'
```

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```yaml
# application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
    tags:
      application: eventmaster
      environment: ${ENVIRONMENT:dev}
```

```java
@Service
public class EventCommandHandler {
    
    private final MeterRegistry metrics;
    private final Counter eventsCreated;
    private final Timer eventCreationTimer;
    
    public EventCommandHandler(MeterRegistry metrics) {
        this.metrics = metrics;
        
        // Custom metrics
        this.eventsCreated = Counter.builder("events.created")
            .description("Total number of events created")
            .tag("type", "command")
            .register(metrics);
        
        this.eventCreationTimer = Timer.builder("event.creation.duration")
            .description("Time to create an event")
            .publishPercentiles(0.5, 0.95, 0.99)  // P50, P95, P99
            .register(metrics);
    }
    
    public void handle(CreateEventCommand cmd) {
        Timer.Sample sample = Timer.start(metrics);
        
        try {
            // Business logic
            Event event = new Event(cmd);
            eventRepository.save(event);
            
            eventBus.publish(new EventCreatedEvent(event));
            
            // Increment counter
            eventsCreated.increment();
            
        } catch (Exception e) {
            // Error counter
            metrics.counter("events.creation.errors", 
                "error", e.getClass().getSimpleName()
            ).increment();
            throw e;
            
        } finally {
            // Record duration
            sample.stop(eventCreationTimer);
        }
    }
}
```

**Key Metrics to Track:**
```java
// Business Metrics
- events.created (counter)
- events.published (counter)
- events.cancelled (counter)
- bookings.created (counter)
- tickets.sold (counter)

// Technical Metrics
- http.server.requests (timer) - HTTP latency
- kafka.producer.record.send.total (counter) - Messages sent
- kafka.consumer.fetch.manager.records.consumed.total - Messages consumed
- jvm.memory.used (gauge) - Memory usage
- jvm.gc.pause (timer) - GC pauses
- hikaricp.connections.active (gauge) - DB connections

// SLI (Service Level Indicators)
- event.creation.duration (P95 < 500ms)
- event.read.duration (P95 < 100ms)
- kafka.consumer.lag (< 1000 messages)
```

#### **2. Logs (Structured Logging with ELK/Loki)**

```xml
<!-- logback-spring.xml -->
<configuration>
    <appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeContext>true</includeContext>
            <includeMdc>true</includeMdc>
            <includeStructuredArguments>true</includeStructuredArguments>
            <fieldNames>
                <timestamp>timestamp</timestamp>
                <message>message</message>
                <logger>logger</logger>
                <thread>thread</thread>
                <level>level</level>
            </fieldNames>
        </encoder>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="JSON"/>
    </root>
</configuration>
```

```java
@Service
@Slf4j
public class EventCommandHandler {
    
    public void handle(CreateEventCommand cmd) {
        // Structured logging
        log.info("Creating event", 
            kv("eventId", cmd.getEventId()),
            kv("name", cmd.getName()),
            kv("userId", cmd.getCreatedBy()),
            kv("action", "EVENT_CREATION_STARTED")
        );
        
        try {
            Event event = createEvent(cmd);
            
            log.info("Event created successfully",
                kv("eventId", event.getId()),
                kv("duration", System.currentTimeMillis() - startTime),
                kv("action", "EVENT_CREATION_COMPLETED")
            );
            
        } catch (Exception e) {
            log.error("Failed to create event",
                kv("eventId", cmd.getEventId()),
                kv("error", e.getClass().getSimpleName()),
                kv("action", "EVENT_CREATION_FAILED"),
                e
            );
            throw e;
        }
    }
}

// Log output (JSON):
{
  "timestamp": "2025-01-19T10:00:00.123Z",
  "level": "INFO",
  "logger": "com.eventmaster.EventCommandHandler",
  "message": "Creating event",
  "eventId": "123e4567-e89b-12d3-a456-426614174000",
  "name": "JavaConf 2025",
  "userId": "user-123",
  "action": "EVENT_CREATION_STARTED",
  "traceId": "abc123",  // From distributed tracing
  "spanId": "def456"
}
```

#### **3. Distributed Tracing (Jaeger/Zipkin)**

```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-api</artifactId>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-jaeger</artifactId>
</dependency>
```

```yaml
# docker-compose.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "5775:5775/udp"
      - "6831:6831/udp"
      - "6832:6832/udp"
      - "5778:5778"
      - "16686:16686"  # UI
      - "14268:14268"
      - "9411:9411"
```

```yaml
# application.yml
management:
  tracing:
    sampling:
      probability: 1.0  # 100% in dev, 0.1 (10%) in prod
  zipkin:
    tracing:
      endpoint: http://jaeger:9411/api/v2/spans
```

```java
@Service
public class EventService {
    
    private final Tracer tracer;
    
    public void createEvent(CreateEventCommand cmd) {
        Span span = tracer.spanBuilder("createEvent")
            .setAttribute("event.name", cmd.getName())
            .setAttribute("user.id", cmd.getUserId().toString())
            .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Business logic
            Event event = repository.save(new Event(cmd));
            
            // Child span for Kafka
            Span kafkaSpan = tracer.spanBuilder("publishToKafka")
                .setAttribute("topic", "events.lifecycle")
                .startSpan();
            try {
                kafkaTemplate.send("events.lifecycle", new EventCreatedEvent(event));
            } finally {
                kafkaSpan.end();
            }
            
            span.addEvent("Event created successfully");
            
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
}

// Trace visualization in Jaeger UI:
// createEvent (200ms)
//   ├─ saveToDatabase (50ms)
//   ├─ publishToKafka (10ms)
//   └─ updateCache (5ms)
```

### Alerting Rules (Prometheus)
```yaml
# alerts.yml
groups:
  - name: eventmaster
    interval: 30s
    rules:
      # High error rate
      - alert: HighErrorRate
        expr: rate(events_creation_errors_total[5m]) > 0.05
        for: 5m
        annotations:
          summary: "High error rate in event creation"
          description: "Error rate is {{ $value }} errors/sec"
      
      # Slow response time
      - alert: SlowEventCreation
        expr: histogram_quantile(0.95, event_creation_duration_seconds) > 0.5
        for: 10m
        annotations:
          summary: "Event creation is slow"
          description: "P95 latency is {{ $value }}s (SLA: 0.5s)"
      
      # Kafka consumer lag
      - alert: HighConsumerLag
        expr: kafka_consumer_lag > 10000
        for: 5m
        annotations:
          summary: "Kafka consumer lag is high"
          description: "Lag: {{ $value }} messages"
      
      # Database connection pool exhausted
      - alert: DatabaseConnectionPoolExhausted
        expr: hikaricp_connections_active / hikaricp_connections_max > 0.9
        for: 2m
        annotations:
          summary: "Database connection pool almost full"
          description: "{{ $value }}% connections used"
```

### Grafana Dashboard (EventMaster)
```json
{
  "dashboard": {
    "title": "EventMaster - Overview",
    "panels": [
      {
        "title": "Request Rate",
        "targets": [{
          "expr": "rate(http_server_requests_seconds_count[5m])"
        }]
      },
      {
        "title": "P95 Latency",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(event_creation_duration_seconds_bucket[5m]))"
        }]
      },
      {
        "title": "Error Rate",
        "targets": [{
          "expr": "rate(events_creation_errors_total[5m])"
        }]
      },
      {
        "title": "Kafka Consumer Lag",
        "targets": [{
          "expr": "kafka_consumer_lag"
        }]
      },
      {
        "title": "JVM Memory",
        "targets": [{
          "expr": "jvm_memory_used_bytes"
        }]
      }
    ]
  }
}
```

### Testowanie Observability
```java
@SpringBootTest
public class ObservabilityTest {
    
    @Autowired
    private MeterRegistry meterRegistry;
    
    @Test
    void shouldRecordMetrics() {
        // Given
        CreateEventCommand cmd = new CreateEventCommand(...);
        
        // When
        eventService.createEvent(cmd);
        
        // Then: Metrics recorded
        Counter counter = meterRegistry.find("events.created").counter();
        assertThat(counter.count()).isEqualTo(1.0);
        
        Timer timer = meterRegistry.find("event.creation.duration").timer();
        assertThat(timer.count()).isEqualTo(1L);
        assertThat(timer.mean(TimeUnit.MILLISECONDS)).isLessThan(500);
    }
    
    @Test
    void shouldRecordDistributedTrace() {
        // Given
        Tracer tracer = openTelemetry.getTracer("test");
        
        // When
        Span span = tracer.spanBuilder("test").startSpan();
        try (Scope scope = span.makeCurrent()) {
            eventService.createEvent(cmd);
        } finally {
            span.end();
        }
        
        // Then: Span exported to Jaeger
        await().untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).isNotEmpty();
            assertThat(spans.get(0).getName()).isEqualTo("createEvent");
        });
    }
}
```

### Best Practices
1. **Use Tags/Labels:** Group metrics by environment, service, endpoint
2. **Monitor SLIs:** Track what matters to users (latency, errors, throughput)
3. **Set SLOs:** P95 latency < 500ms, Error rate < 0.1%
4. **Alert on Symptoms, Not Causes:** Alert on "slow requests" not "high CPU"
5. **Structured Logs:** Always use JSON logging in production

---

## Temat 45: Error Handling Strategies - Obsługa Błędów

### Definicja
W systemach rozproszonych **błędy są normą**, nie wyjątkiem. Trzeba je przewidywać i elegancko obsługiwać.

### Typy Błędów
1. **Transient** (przejściowe) - network timeout, DB connection lost → RETRY
2. **Permanent** (trwałe) - invalid data, business rule violation → NO RETRY
3. **Systematic** (systemowe) - service down, out of memory → CIRCUIT BREAKER

### Strategie Obsługi

#### **1. Retry with Exponential Backoff**
```java
@Service
public class EventProjector {
    
    @KafkaListener(topics = "events.lifecycle")
    @Retryable(
        value = {TransientDataAccessException.class, KafkaException.class},
        maxAttempts = 3,
        backoff = @Backoff(
            delay = 1000,      // Start with 1s
            multiplier = 2.0,  // Double each time
            maxDelay = 10000   // Max 10s
        )
    )
    public void projectEvent(EventCreatedEvent event) {
        EventView view = new EventView(event);
        repository.save(view);
    }
    
    @Recover
    public void recover(TransientDataAccessException e, EventCreatedEvent event) {
        dlqService.sendToDlq(event, e);
    }
}
```

#### **2. Circuit Breaker**
```java
@Service
public class EmailNotificationService {
    
    @CircuitBreaker(name = "emailService", fallbackMethod = "fallbackSendEmail")
    public void sendEventCreatedEmail(EventCreatedEvent event) {
        emailClient.send(event.getOrganizerEmail(), "Event Created", buildEmailBody(event));
    }
    
    public void fallbackSendEmail(EventCreatedEvent event, Exception e) {
        emailQueueRepository.save(new EmailQueueEntry(event));
    }
}
```

#### **3. Saga Pattern (Compensating Actions)**
```java
@Service
public class BookingOrchestrator {
    
    @Transactional
    public void bookTickets(BookTicketsCommand cmd) {
        Saga saga = new Saga();
        
        try {
            Booking booking = bookingService.create(cmd);
            saga.addCompensation(() -> bookingService.cancel(booking.getId()));
            
            Reservation reservation = ticketService.reserve(cmd.getTicketIds());
            saga.addCompensation(() -> ticketService.releaseReservation(reservation.getId()));
            
            Payment payment = paymentService.charge(cmd.getPaymentMethod(), cmd.getAmount());
            saga.addCompensation(() -> paymentService.refund(payment.getId()));
            
            saga.complete();
        } catch (Exception e) {
            saga.compensate();
            throw new BookingFailedException("Failed to book tickets", e);
        }
    }
}
```

**Testowanie:**
```java
@Test
void saga_shouldCompensateOnFailure() {
    doThrow(new PaymentFailedException("Insufficient funds"))
        .when(mockPaymentService).charge(any(), any());
    
    assertThatThrownBy(() -> orchestrator.bookTickets(cmd))
        .isInstanceOf(BookingFailedException.class);
    
    verify(mockBookingService).cancel(any());
    verify(mockTicketService).releaseReservation(any());
}
```

---

## Temat 46: Deployment Strategies - Strategie Wdrażania

### Definicja
**Deployment Strategy** to sposób wdrażania nowych wersji aplikacji z minimalnym ryzykiem i downtime.

### 1. Blue-Green Deployment

**Koncepcja:**
- Blue (stara wersja) działa w production
- Green (nowa wersja) deploy równolegle
- Switch traffic: Blue → Green (instant cutover)
- Rollback łatwy: Green → Blue

**W EventMaster z Docker & Caddy:**
```yaml
# docker-compose.blue-green.yml
services:
  backend-blue:
    image: eventmaster-backend:v1.0.0
    environment:
      - ENVIRONMENT=blue
    labels:
      - "caddy=api-blue.eventmaster.local"
  
  backend-green:
    image: eventmaster-backend:v1.1.0
    environment:
      - ENVIRONMENT=green
    labels:
      - "caddy=api-green.eventmaster.local"
  
  caddy:
    image: caddy:2-alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
```

```caddyfile
# Caddyfile - Switch by env variable
api.eventmaster.local {
    reverse_proxy {
        {$ACTIVE_ENV}:8080
    }
}

# Switch deployment:
# ACTIVE_ENV=backend-blue → Blue active
# ACTIVE_ENV=backend-green → Green active
```

**Deployment Script:**
```bash
#!/bin/bash
# deploy-blue-green.sh

CURRENT_ENV=$(cat /var/current-env)
NEW_ENV=$([ "$CURRENT_ENV" == "blue" ] && echo "green" || echo "blue")

echo "Current: $CURRENT_ENV, Deploying to: $NEW_ENV"

# 1. Deploy new version to inactive environment
docker-compose up -d backend-$NEW_ENV

# 2. Health check
for i in {1..30}; do
    if curl -f http://backend-$NEW_ENV:8080/actuator/health; then
        echo "Health check passed"
        break
    fi
    sleep 2
done

# 3. Switch traffic
export ACTIVE_ENV=backend-$NEW_ENV
docker-compose restart caddy

# 4. Verify
sleep 5
if curl -f http://api.eventmaster.local/actuator/health; then
    echo "Deployment successful"
    echo $NEW_ENV > /var/current-env
    
    # Keep old version running for 10 minutes (rollback window)
    sleep 600
    docker-compose stop backend-$CURRENT_ENV
else
    echo "Deployment failed, rolling back"
    export ACTIVE_ENV=backend-$CURRENT_ENV
    docker-compose restart caddy
    exit 1
fi
```

### 2. Canary Deployment

**Koncepcja:**
- Deploy new version to small subset (5%)
- Monitor metrics (errors, latency)
- Gradually increase: 5% → 25% → 50% → 100%
- Rollback if metrics degrade

**Caddy Configuration:**
```caddyfile
api.eventmaster.local {
    # Weighted load balancing (Canary: 5% to v1.1.0)
    reverse_proxy backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-0-0:8080 backend-v1-0-0:8080 \
                  backend-v1-0-0:8080 backend-v1-1-0:8080  # 5% canary
}
```

**Automated Canary with Prometheus:**
```python
# canary_controller.py
import requests
import time

def get_error_rate(version):
    query = f'rate(http_server_requests_seconds_count{{status=~"5..", version="{version}"}}[5m])'
    response = requests.get(f'http://prometheus:9090/api/v1/query?query={query}')
    return float(response.json()['data']['result'][0]['value'][1])

def deploy_canary():
    stages = [5, 10, 25, 50, 100]  # Traffic percentages
    
    for stage in stages:
        print(f"Canary at {stage}%")
        update_traffic_split(stage)
        
        time.sleep(300)  # Monitor for 5 minutes
        
        error_rate_old = get_error_rate("v1.0.0")
        error_rate_new = get_error_rate("v1.1.0")
        
        if error_rate_new > error_rate_old * 1.5:  # 50% more errors
            print("Canary showing elevated errors. Rolling back.")
            update_traffic_split(0)  # Rollback to 0%
            return False
        
        print(f"Stage {stage}% successful")
    
    print("Canary deployment completed successfully")
    return True
```

### 3. Rolling Deployment

**Koncepcja:**
- Update instances one by one
- Always N-1 instances available
- Gradual rollout

**Kubernetes Example:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventmaster-backend
spec:
  replicas: 5
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1        # 1 extra pod during update
      maxUnavailable: 1  # Max 1 pod down at a time
  template:
    spec:
      containers:
      - name: backend
        image: eventmaster-backend:v1.1.0
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
```

### 4. Recreate Deployment (Downtime OK)

**Koncepcja:**
- Stop wszystkie stare instancje
- Start nowe instancje
- Downtime: 30-60 sekund

```bash
# Simple recreate
docker-compose down backend
docker-compose up -d backend
```

### Testowanie Deployment Strategies
```java
@SpringBootTest
public class DeploymentStrategyTest {
    
    @Test
    void blueGreen_shouldSwitchTrafficWithoutErrors() {
        // Given: Blue version running
        startBlueVersion();
        assertThat(sendRequest()).isEqualTo("Blue v1.0.0");
        
        // When: Deploy green
        startGreenVersion();
        waitForHealthCheck("green");
        
        // Then: Can switch to green
        switchToGreen();
        assertThat(sendRequest()).isEqualTo("Green v1.1.0");
        
        // And: Can rollback
        switchToBlue();
        assertThat(sendRequest()).isEqualTo("Blue v1.0.0");
    }
    
    @Test
    void canary_shouldRollbackOnHighErrorRate() {
        // Given: v1.0.0 with 1% error rate
        deployVersion("v1.0.0");
        simulateErrorRate(0.01);
        
        // When: Deploy v1.1.0 canary with 10% error rate (bad!)
        deployCan ary("v1.1.0", percentage = 5);
        simulateErrorRate(0.10, version = "v1.1.0");
        
        // Then: Canary controller detects and rollbacks
        await().atMost(30, SECONDS).untilAsserted(() -> {
            assertThat(getTrafficPercentage("v1.1.0")).isEqualTo(0);
        });
    }
}
```

### Database Migrations During Deployment
```sql
-- Backward-compatible migration
-- Step 1: Add new column (nullable)
ALTER TABLE events ADD COLUMN capacity INTEGER;

-- Step 2: Deploy code v1.1.0 (writes to both old + new schema)

-- Step 3: Backfill data
UPDATE events SET capacity = 100 WHERE capacity IS NULL;

-- Step 4: Make column NOT NULL (after all instances updated)
ALTER TABLE events ALTER COLUMN capacity SET NOT NULL;
```

### Best Practices
1. **Health Checks:** Always verify new version before switching traffic
2. **Gradual Rollout:** Start small (5%), monitor, increase
3. **Automated Rollback:** Detect issues and rollback automatically
4. **Database Migrations:** Backward-compatible, multi-step
5. **Feature Flags:** Toggle new features without redeployment

---

## Temat 47: Scaling CQRS Systems - Skalowanie

### Horizontal Scaling (Scale Out)

**Write Side (Command Handler):**
```yaml
# docker-compose.scale.yml
services:
  backend:
    image: eventmaster-backend:latest
    deploy:
      replicas: 5  # 5 instances
    environment:
      - KAFKA_GROUP_ID=event-commands
```

```bash
# Scale dynamically
docker-compose up -d --scale backend=10
```

**Read Side (Query Handler):**
```yaml
services:
  read-api:
    image: eventmaster-read-api:latest
    deploy:
      replicas: 10  # More replicas for reads (80% traffic)
```

### Database Scaling Strategies

#### **1. Read Replicas (PostgreSQL 18)**
```yaml
services:
  postgres-primary:
    image: postgres:18-alpine
    environment:
      POSTGRES_PASSWORD: password
    volumes:
      - ./pg-primary.conf:/etc/postgresql/postgresql.conf
  
  postgres-replica1:
    image: postgres:18-alpine
    environment:
      POSTGRES_MASTER_SERVICE: postgres-primary
      POSTGRES_PASSWORD: password
    command: |
      postgres -c 'hot_standby=on' \
               -c 'primary_conninfo=host=postgres-primary port=5432'
```

**Application Configuration:**
```yaml
# application.yml
spring:
  datasource:
    hikari:
      primary:
        jdbc-url: jdbc:postgresql://postgres-primary:5432/eventmaster
        maximum-pool-size: 20
      replica:
        jdbc-url: jdbc:postgresql://postgres-replica1:5432/eventmaster
        maximum-pool-size: 50  # More connections for reads
```

```java
@Configuration
public class DataSourceConfig {
    
    @Bean
    @Primary
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create()
            .url("jdbc:postgresql://postgres-primary:5432/eventmaster")
            .build();
    }
    
    @Bean
    public DataSource replicaDataSource() {
        return DataSourceBuilder.create()
            .url("jdbc:postgresql://postgres-replica1:5432/eventmaster")
            .build();
    }
    
    @Bean
    public DataSource routingDataSource(
        @Qualifier("primaryDataSource") DataSource primary,
        @Qualifier("replicaDataSource") DataSource replica
    ) {
        Map<Object, Object> sources = new HashMap<>();
        sources.put(RoutingKey.PRIMARY, primary);
        sources.put(RoutingKey.REPLICA, replica);
        
        RoutingDataSource routing = new RoutingDataSource();
        routing.setTargetDataSources(sources);
        routing.setDefaultTargetDataSource(primary);
        return routing;
    }
}

// Read from replica
@Transactional(readOnly = true)
@RoutingDataSource(RoutingKey.REPLICA)
public List<EventView> findAllEvents() {
    return eventViewRepository.findAll();  // Read from replica
}

// Write to primary
@Transactional
@RoutingDataSource(RoutingKey.PRIMARY)
public void createEvent(CreateEventCommand cmd) {
    Event event = new Event(cmd);
    eventRepository.save(event);  // Write to primary
}
```

#### **2. Partitioning/Sharding**
```sql
-- Partition by date (PostgreSQL 18 declarative partitioning)
CREATE TABLE events (
    id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    ...
) PARTITION BY RANGE (created_at);

CREATE TABLE events_2024 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE events_2025 PARTITION OF events
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Queries automatically route to correct partition
SELECT * FROM events WHERE created_at >= '2025-06-01';  -- Uses events_2025
```

### Kafka Scaling

**Increase Partitions:**
```bash
# Increase from 3 to 10 partitions
kafka-topics --bootstrap-server kafka:9092 \
             --alter --topic events.lifecycle \
             --partitions 10
```

**Scale Consumers:**
```yaml
spring:
  kafka:
    listener:
      concurrency: 5  # 5 consumer threads per instance
```

```bash
# Scale to 10 instances × 5 threads = 50 concurrent consumers
docker-compose up -d --scale backend=10
```

### Caching (Redis)
```yaml
services:
  redis:
    image: redis:7-alpine
    command: redis-server --maxmemory 2gb --maxmemory-policy allkeys-lru
```

```java
@Configuration
@EnableCaching
public class CacheConfig {
    
    @Bean
    public RedisCacheManager cacheManager(RedisConnectionFactory factory) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
            .entryTtl(Duration.ofMinutes(10))
            .serializeValuesWith(RedisSerializationContext.SerializationPair
                .fromSerializer(new GenericJackson2JsonRedisSerializer()));
        
        return RedisCacheManager.builder(factory)
            .cacheDefaults(config)
            .build();
    }
}

@Service
public class EventQueryService {
    
    @Cacheable(value = "events", key = "#id")
    public EventDTO findById(UUID id) {
        // Cache miss → query database
        // Cache hit → return from Redis (fast!)
        return eventViewRepository.findById(id)
            .map(this::toDTO)
            .orElseThrow();
    }
    
    @CacheEvict(value = "events", key = "#event.eventId")
    @KafkaListener(topics = "events.lifecycle")
    public void onEventUpdated(EventUpdatedEvent event) {
        // Invalidate cache on update
    }
}
```

### Load Testing
```scala
// Gatling load test
class EventMasterLoadTest extends Simulation {
    
    val httpProtocol = http
        .baseUrl("http://localhost:8080")
        .acceptHeader("application/json")
    
    val createEventScenario = scenario("Create Events")
        .exec(
            http("Create Event")
                .post("/api/v1/events")
                .body(StringBody("""{"name": "Event ${eventId}"}""")).asJson
                .check(status.is(201))
        )
    
    val readEventScenario = scenario("Read Events")
        .exec(
            http("List Events")
                .get("/api/v1/events")
                .check(status.is(200))
        )
    
    setUp(
        createEventScenario.inject(
            rampUsersPerSec(10) to 100 during (60 seconds)  // Writes: 10→100 rps
        ),
        readEventScenario.inject(
            rampUsersPerSec(100) to 1000 during (60 seconds)  // Reads: 100→1000 rps
        )
    ).protocols(httpProtocol)
}
```

### Best Practices
1. **Measure First:** Don't optimize prematurely
2. **Horizontal > Vertical:** Scale out, not up
3. **Cache Aggressively:** Read Model should be mostly cached
4. **Partition Data:** Shard by tenant, date, or geographic region
5. **Monitor Bottlenecks:** Database? Kafka? Network?

---

## Temat 48: Database per Service - Baza na Serwis

### Zasada (Microservices Pattern)
Każdy **Bounded Context** ma swoją własną bazę danych. Nie dzielą się bazą!

### Dlaczego?
1. **Niezależność Deploymentu** - można zmieniać schemat DB bez wpływu na inne serwisy
2. **Technologia Heterogeniczna** - Event Management → PostgreSQL, Analytics → MongoDB
3. **Izolacja Awarii** - crash jednej bazy nie wpływa na inne
4. **Skalowanie Niezależne** - każda baza skaluje się osobno

### W EventMaster - Architecture
```
┌─────────────────────────────────────────┐
│    EVENT MANAGEMENT SERVICE             │
│  ┌───────────────────────────────┐     │
│  │  PostgreSQL 18 (Primary)      │     │
│  │  - events                     │     │
│  │  - event_dates                │     │
│  │  - locations                  │     │
│  └───────────────────────────────┘     │
└─────────────────────────────────────────┘
                 ↓ Events (Kafka)
┌─────────────────────────────────────────┐
│    TICKETING SERVICE                    │
│  ┌───────────────────────────────┐     │
│  │  PostgreSQL 18 (Separate)     │     │
│  │  - tickets                    │     │
│  │  - reservations               │     │
│  │  - inventory                  │     │
│  └───────────────────────────────┘     │
└─────────────────────────────────────────┘
                 ↓ Events (Kafka)
┌─────────────────────────────────────────┐
│    ANALYTICS SERVICE                    │
│  ┌───────────────────────────────┐     │
│  │  MongoDB                       │     │
│  │  - event_views                │     │
│  │  - user_interactions          │     │
│  │  - conversion_metrics         │     │
│  └───────────────────────────────┘     │
└─────────────────────────────────────────┘
```

### Docker Compose Setup
```yaml
# docker-compose.yml
services:
  # Event Management Database
  postgres-events:
    image: postgres:18-alpine
    environment:
      POSTGRES_DB: events_db
      POSTGRES_PASSWORD: events_pass
    volumes:
      - events-data:/var/lib/postgresql/data
    networks:
      - event-management-net
  
  # Ticketing Database (Separate!)
  postgres-ticketing:
    image: postgres:18-alpine
    environment:
      POSTGRES_DB: ticketing_db
      POSTGRES_PASSWORD: ticketing_pass
    volumes:
      - ticketing-data:/var/lib/postgresql/data
    networks:
      - ticketing-net
  
  # Analytics Database (Different tech!)
  mongo-analytics:
    image: mongo:7
    environment:
      MONGO_INITDB_DATABASE: analytics_db
    volumes:
      - analytics-data:/data/db
    networks:
      - analytics-net
  
  # Event Management Service
  event-management-service:
    build: ./event-management
    environment:
      DB_URL: jdbc:postgresql://postgres-events:5432/events_db
      KAFKA_BOOTSTRAP: kafka:9092
    networks:
      - event-management-net
      - kafka-net
  
  # Ticketing Service (NO ACCESS to postgres-events!)
  ticketing-service:
    build: ./ticketing
    environment:
      DB_URL: jdbc:postgresql://postgres-ticketing:5432/ticketing_db
      KAFKA_BOOTSTRAP: kafka:9092
    networks:
      - ticketing-net  # Separate network!
      - kafka-net
  
  # Analytics Service
  analytics-service:
    build: ./analytics
    environment:
      MONGO_URI: mongodb://mongo-analytics:27017/analytics_db
      KAFKA_BOOTSTRAP: kafka:9092
    networks:
      - analytics-net
      - kafka-net

volumes:
  events-data:
  ticketing-data:
  analytics-data:

networks:
  event-management-net:
  ticketing-net:
  analytics-net:
  kafka-net:
```

### Cross-Service Data Access (ANTI-PATTERN!)
```java
// ❌ BAD: Direct database access across services
@Service
public class TicketingService {
    @Autowired
    private JdbcTemplate eventsDb;  // Wrong! Accessing Event Management DB!
    
    public void reserveTickets(UUID eventId) {
        // Querying other service's database!
        String eventName = eventsDb.queryForObject(
            "SELECT name FROM events WHERE id = ?", 
            String.class, 
            eventId
        );
        // BAD! Tight coupling!
    }
}
```

### Correct Way: Communication via Events
```java
// ✅ GOOD: Event-driven communication
@Service
public class TicketingProjector {
    
    @KafkaListener(topics = "events.lifecycle", groupId = "ticketing-service")
    public void onEventCreated(EventCreatedEvent event) {
        // Ticketing service maintains its OWN copy
        TicketingEvent ticketingEvent = new TicketingEvent(
            event.getEventId(),
            event.getCapacity()  // Only data it needs
        );
        ticketingEventRepository.save(ticketingEvent);
    }
}

// Now Ticketing service has its own data
@Service
public class TicketingService {
    
    public void reserveTickets(UUID eventId, int quantity) {
        // Query OWN database
        TicketingEvent event = ticketingEventRepository.findById(eventId)
            .orElseThrow(() -> new EventNotFoundException(eventId));
        
        if (event.getRemainingCapacity() < quantity) {
            throw new InsufficientCapacityException();
        }
        
        // Reserve
        Reservation reservation = new Reservation(eventId, quantity);
        reservationRepository.save(reservation);
    }
}
```

### Handling Joins Across Services
**Problem:** "Show events with ticket availability"  
**Solution:** API Composition or CQRS Read Model

#### **Option 1: API Composition (Backend)**
```java
@RestController
public class EventCompositionController {
    
    @Autowired
    private EventServiceClient eventClient;
    
    @Autowired
    private TicketServiceClient ticketClient;
    
    @GetMapping("/api/v1/events-with-tickets")
    public List<EventWithTicketsDTO> getEventsWithTickets() {
        // Call Event Management Service
        List<EventDTO> events = eventClient.getAllEvents();
        
        // Call Ticketing Service for each event
        return events.stream()
            .map(event -> {
                TicketAvailability availability = 
                    ticketClient.getAvailability(event.getId());
                
                return new EventWithTicketsDTO(
                    event,
                    availability.getRemainingTickets()
                );
            })
            .collect(toList());
    }
}

// ⚠️ Problem: N+1 queries (slow for 100 events!)
```

#### **Option 2: CQRS Read Model (Better!)**
```java
// Dedicated Read Model service with denormalized data
@Service
public class EventListProjector {
    
    @Autowired
    private EventListViewRepository repository;
    
    @KafkaListener(topics = "events.lifecycle")
    public void onEventCreated(EventCreatedEvent event) {
        EventListView view = new EventListView(
            event.getEventId(),
            event.getName(),
            event.getCapacity(),  // From Event Service
            event.getCapacity()   // Initial remaining = capacity
        );
        repository.save(view);
    }
    
    @KafkaListener(topics = "ticketing.reservations")
    public void onTicketReserved(TicketReservedEvent event) {
        EventListView view = repository.findById(event.getEventId()).orElseThrow();
        view.setRemainingTickets(view.getRemainingTickets() - event.getQuantity());
        repository.save(view);
    }
}

// Now single query!
@GetMapping("/api/v1/events-with-tickets")
public List<EventListView> getEventsWithTickets() {
    return repository.findAll();  // One query! 🚀
}
```

### Data Consistency Challenges

#### **Challenge 1: Distributed Transactions**
**Problem:** Create event + reserve tickets (atomic?)

**Solution: Saga Pattern**
```java
@Service
public class EventBookingSaga {
    
    public void bookEvent(BookEventCommand cmd) {
        // Step 1: Create event
        UUID eventId = eventService.createEvent(cmd.getEventDetails());
        
        // Step 2: Reserve tickets (different service!)
        try {
            ticketService.reserveTickets(eventId, cmd.getTicketQuantity());
        } catch (Exception e) {
            // Compensate: Cancel event
            eventService.cancelEvent(eventId);
            throw new BookingFailedException("Failed to reserve tickets", e);
        }
    }
}
```

#### **Challenge 2: Eventual Consistency**
**Problem:** Event created but Ticketing service hasn't synced yet

**Solution:** Accept and handle**
```java
@GetMapping("/api/v1/events/{id}/tickets")
public ResponseEntity<TicketDTO> getTickets(@PathVariable UUID id) {
    Optional<TicketingEvent> event = ticketingEventRepo.findById(id);
    
    if (event.isEmpty()) {
        // Event not yet synced from Event Management service
        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .header("Retry-After", "5")
            .body(new ErrorDTO("Event data is being synchronized. Retry in 5 seconds."));
    }
    
    return ResponseEntity.ok(toDTO(event.get()));
}
```

### Testing Database Isolation
```java
@SpringBootTest
@Testcontainers
public class DatabaseIsolationTest {
    
    @Container
    static PostgreSQLContainer<?> eventsDb = 
        new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("events_db");
    
    @Container
    static PostgreSQLContainer<?> ticketingDb = 
        new PostgreSQLContainer<>("postgres:18-alpine")
            .withDatabaseName("ticketing_db");
    
    @Test
    void services_shouldNotAccessEachOthersDatabases() {
        // Event Management can't access Ticketing DB
        assertThatThrownBy(() -> 
            eventsDataSource.getConnection()
                .prepareStatement("SELECT * FROM tickets")
                .executeQuery()
        ).isInstanceOf(SQLException.class)
         .hasMessageContaining("relation \"tickets\" does not exist");
        
        // Ticketing can't access Events DB
        assertThatThrownBy(() -> 
            ticketingDataSource.getConnection()
                .prepareStatement("SELECT * FROM events")
                .executeQuery()
        ).isInstanceOf(SQLException.class)
         .hasMessageContaining("relation \"events\" does not exist");
    }
}
```

### Best Practices
1. **Strict Boundaries:** Enforce network isolation (Docker networks)
2. **Event-Driven Sync:** Services sync data via events, not direct DB access
3. **Eventual Consistency:** Accept that data won't be immediately consistent
4. **Denormalize for Reads:** CQRS Read Models join data from multiple services
5. **Saga for Transactions:** Use Saga pattern for multi-service transactions

---

## Temat 49: API Versioning - Wersjonowanie API

### Dlaczego Versioning?
**Problem:** Breaking changes w API łamią existing clients

**Example:**
```json
// V1 API (2024)
GET /api/events/123
{
  "id": "123",
  "name": "JavaConf"
}

// V2 API (2025) - Breaking change!
GET /api/events/123
{
  "eventId": "123",      // ← Changed field name!
  "eventName": "JavaConf",  // ← Changed field name!
  "capacity": 500        // ← New required field
}

// Mobile app (V1 client) crashes! 💥
```

### Strategie Versioning

#### **1. URL Versioning (Zalecane dla REST)**
```java
// V1 Controller
@RestController
@RequestMapping("/api/v1/events")
public class EventControllerV1 {
    
    @GetMapping("/{id}")
    public EventDTOV1 getEvent(@PathVariable UUID id) {
        return new EventDTOV1(
            event.getId(),
            event.getName()
        );
    }
}

// V2 Controller (nowa wersja)
@RestController
@RequestMapping("/api/v2/events")
public class EventControllerV2 {
    
    @GetMapping("/{id}")
    public EventDTOV2 getEvent(@PathVariable UUID id) {
        return new EventDTOV2(
            event.getId(),
            event.getName(),
            event.getCapacity()  // New field
        );
    }
}

// Clients choose version:
// curl http://api.eventmaster.com/api/v1/events/123  → V1
// curl http://api.eventmaster.com/api/v2/events/123  → V2
```

**Zalety:**
- ✅ Proste (URL wyraźnie pokazuje wersję)
- ✅ Łatwe cachowanie (różne URL-e)
- ✅ Łatwe testowanie

**Wady:**
- ❌ Duplikacja kodu (dwa kontrolery)

#### **2. Header Versioning**
```java
@RestController
@RequestMapping("/api/events")
public class EventController {
    
    @GetMapping(value = "/{id}", headers = "API-Version=1")
    public EventDTOV1 getEventV1(@PathVariable UUID id) {
        return toV1DTO(event);
    }
    
    @GetMapping(value = "/{id}", headers = "API-Version=2")
    public EventDTOV2 getEventV2(@PathVariable UUID id) {
        return toV2DTO(event);
    }
}

// Client specifies version in header:
// curl -H "API-Version: 1" http://api.eventmaster.com/api/events/123
// curl -H "API-Version: 2" http://api.eventmaster.com/api/events/123
```

#### **3. Content Negotiation (Accept Header)**
```java
@GetMapping(
    value = "/{id}", 
    produces = "application/vnd.eventmaster.v1+json"
)
public EventDTOV1 getEventV1(@PathVariable UUID id) {
    return toV1DTO(event);
}

@GetMapping(
    value = "/{id}", 
    produces = "application/vnd.eventmaster.v2+json"
)
public EventDTOV2 getEventV2(@PathVariable UUID id) {
    return toV2DTO(event);
}

// Client:
// curl -H "Accept: application/vnd.eventmaster.v1+json" ...
```

### Non-Breaking Changes (No Version Bump)
```json
// V1 API
{
  "id": "123",
  "name": "JavaConf"
}

// V1.1 API (backward compatible)
{
  "id": "123",
  "name": "JavaConf",
  "capacity": 500  // ← Optional new field (V1 clients ignore it)
}

// ✅ V1 clients still work!
```

### Deprecation Strategy
```java
@RestController
@RequestMapping("/api/v1/events")
@Deprecated  // Mark as deprecated
public class EventControllerV1 {
    
    @GetMapping("/{id}")
    public ResponseEntity<EventDTOV1> getEvent(@PathVariable UUID id) {
        EventDTOV1 dto = toV1DTO(event);
        
        return ResponseEntity.ok()
            .header("Deprecation", "true")
            .header("Sunset", "2025-12-31")  // Shutdown date
            .header("Link", "</api/v2/events>; rel=\"successor-version\"")
            .body(dto);
    }
}

// Response:
// HTTP/1.1 200 OK
// Deprecation: true
// Sunset: 2025-12-31
// Link: </api/v2/events>; rel="successor-version"
```

### Testowanie API Versioning
```java
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class APIVersioningTest {
    
    @Test
    void v1_shouldReturnV1Format() {
        // When
        ResponseEntity<String> response = restTemplate.getForEntity(
            "/api/v1/events/{id}",
            String.class,
            testEventId
        );
        
        // Then
        JsonNode json = objectMapper.readTree(response.getBody());
        assertThat(json.has("id")).isTrue();
        assertThat(json.has("name")).isTrue();
        assertThat(json.has("capacity")).isFalse();  // V1 doesn't have this
    }
    
    @Test
    void v2_shouldReturnV2Format() {
        // When
        ResponseEntity<String> response = restTemplate.getForEntity(
            "/api/v2/events/{id}",
            String.class,
            testEventId
        );
        
        // Then
        JsonNode json = objectMapper.readTree(response.getBody());
        assertThat(json.has("id")).isTrue();
        assertThat(json.has("name")).isTrue();
        assertThat(json.has("capacity")).isTrue();  // V2 has this
    }
    
    @Test
    void v1_shouldStillWorkAfterV2Released() {
        // Given: V2 deployed
        // When: Old V1 client makes request
        ResponseEntity<EventDTOV1> response = v1Client.getEvent(testEventId);
        
        // Then: V1 response (backward compatible)
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getId()).isEqualTo(testEventId);
    }
}
```

### Best Practices
1. **Major version in URL:** `/api/v1/`, `/api/v2/`
2. **Backward Compatible:** Add fields, don't remove
3. **Deprecation Period:** 6-12 months before removal
4. **Document:** Clear migration guide V1 → V2
5. **Monitor Usage:** Track which versions are still used

---

## Temat 50: Performance Testing - Testy Wydajnościowe

### Definicja
**Performance Testing** weryfikuje, jak system zachowuje się pod obciążeniem.

### Typy Testów
1. **Load Testing** - normalny ruch (100 users)
2. **Stress Testing** - ekstremalny ruch (1000 users)
3. **Spike Testing** - nagły skok (10 → 1000 users w 10s)
4. **Soak Testing** - długotrwały ruch (24h non-stop)

### Metryki (SLI - Service Level Indicators)
- **Throughput:** Requests per second (RPS)
- **Latency:** P50, P95, P99 response time
- **Error Rate:** % failed requests
- **Saturation:** CPU, Memory, Disk usage

### SLO (Service Level Objectives) dla EventMaster
```
✅ P95 latency < 500ms dla GET /events
✅ P99 latency < 1000ms dla POST /events
✅ Throughput > 100 RPS dla read operations
✅ Error rate < 0.1% (99.9% success)
✅ System handles 10,000 concurrent users
```

### Narzędzia

#### **1. Gatling (Scala-based)**
```scala
// EventMasterLoadTest.scala
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

class EventMasterLoadTest extends Simulation {
    
    val httpProtocol = http
        .baseUrl("http://localhost:8080")
        .acceptHeader("application/json")
        .contentTypeHeader("application/json")
    
    // Scenario 1: Browse events
    val browseEvents = scenario("Browse Events")
        .exec(
            http("List Events")
                .get("/api/v1/events")
                .check(status.is(200))
                .check(jsonPath("$[*].id").findAll.saveAs("eventIds"))
        )
        .pause(1.second)
        .exec(
            http("Get Event Details")
                .get("/api/v1/events/${eventIds.random()}")
                .check(status.is(200))
        )
    
    // Scenario 2: Create events
    val createEvents = scenario("Create Events")
        .exec(
            http("Create Event")
                .post("/api/v1/events")
                .body(StringBody("""
                    {
                        "name": "Event ${eventId}",
                        "location": "Warsaw, Poland",
                        "startDate": "2025-06-01T10:00:00"
                    }
                """)).asJson
                .check(status.is(201))
                .check(jsonPath("$.id").saveAs("createdEventId"))
        )
    
    // Load profile
    setUp(
        browseEvents.inject(
            nothingFor(4.seconds),
            atOnceUsers(10),
            rampUsers(50) during (30.seconds),
            constantUsersPerSec(20) during (60.seconds),
            rampUsersPerSec(20) to 100 during (120.seconds)
        ),
        createEvents.inject(
            rampUsersPerSec(1) to 10 during (60.seconds)
        )
    ).protocols(httpProtocol)
     .assertions(
        global.responseTime.percentile(95).lt(500),  // P95 < 500ms
        global.successfulRequests.percent.gt(99.9)    // 99.9% success
     )
}

// Run:
// mvn gatling:test -Dgatling.simulationClass=EventMasterLoadTest
```

#### **2. JMeter (GUI + CLI)**
```xml
<!-- EventMaster.jmx -->
<jmeterTestPlan>
    <ThreadGroup guiclass="ThreadGroupGui" testname="Event Management">
        <stringProp name="ThreadGroup.num_threads">100</stringProp>
        <stringProp name="ThreadGroup.ramp_time">60</stringProp>
        <stringProp name="ThreadGroup.duration">300</stringProp>
        
        <HTTPSamplerProxy>
            <stringProp name="HTTPSampler.domain">localhost</stringProp>
            <stringProp name="HTTPSampler.port">8080</stringProp>
            <stringProp name="HTTPSampler.path">/api/v1/events</stringProp>
            <stringProp name="HTTPSampler.method">GET</stringProp>
        </HTTPSamplerProxy>
    </ThreadGroup>
</jmeterTestPlan>

<!-- Run: -->
<!-- jmeter -n -t EventMaster.jmx -l results.jtl -->
```

#### **3. K6 (Modern, JavaScript-based)**
```javascript
// eventmaster-load-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
    stages: [
        { duration: '30s', target: 10 },   // Ramp up to 10 users
        { duration: '1m', target: 50 },    // Ramp up to 50 users
        { duration: '2m', target: 100 },   // Ramp up to 100 users
        { duration: '1m', target: 0 },     // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],  // P95 < 500ms
        errors: ['rate<0.01'],              // Error rate < 1%
    },
};

export default function() {
    // GET /events
    let getRes = http.get('http://localhost:8080/api/v1/events');
    check(getRes, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    }) || errorRate.add(1);
    
    sleep(1);
    
    // POST /events
    let payload = JSON.stringify({
        name: `Event ${__VU}-${__ITER}`,  // Unique per virtual user & iteration
        location: 'Warsaw, Poland',
        startDate: '2025-06-01T10:00:00'
    });
    
    let postRes = http.post('http://localhost:8080/api/v1/events', payload, {
        headers: { 'Content-Type': 'application/json' },
    });
    
    check(postRes, {
        'created successfully': (r) => r.status === 201,
    }) || errorRate.add(1);
    
    sleep(2);
}

// Run:
// k6 run eventmaster-load-test.js
```

### Testowanie w EventMaster
```java
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
public class PerformanceTest {
    
    @Test
    @Disabled("Run manually for performance testing")
    void loadTest_shouldHandle100ConcurrentUsers() throws Exception {
        int concurrentUsers = 100;
        int requestsPerUser = 100;
        ExecutorService executor = Executors.newFixedThreadPool(concurrentUsers);
        
        CountDownLatch latch = new CountDownLatch(concurrentUsers * requestsPerUser);
        List<Long> responseTimes = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger(0);
        
        // Start load test
        Instant start = Instant.now();
        
        for (int i = 0; i < concurrentUsers; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerUser; j++) {
                    try {
                        Instant requestStart = Instant.now();
                        
                        ResponseEntity<String> response = restTemplate.getForEntity(
                            "/api/v1/events",
                            String.class
                        );
                        
                        Instant requestEnd = Instant.now();
                        responseTimes.add(Duration.between(requestStart, requestEnd).toMillis());
                        
                        if (response.getStatusCode() != HttpStatus.OK) {
                            errors.incrementAndGet();
                        }
                        
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        // Wait for completion
        latch.await(5, TimeUnit.MINUTES);
        executor.shutdown();
        
        Instant end = Instant.now();
        long totalDuration = Duration.between(start, end).toMillis();
        
        // Calculate metrics
        Collections.sort(responseTimes);
        long p50 = responseTimes.get(responseTimes.size() / 2);
        long p95 = responseTimes.get((int) (responseTimes.size() * 0.95));
        long p99 = responseTimes.get((int) (responseTimes.size() * 0.99));
        double throughput = (concurrentUsers * requestsPerUser * 1000.0) / totalDuration;
        double errorRate = (errors.get() * 100.0) / (concurrentUsers * requestsPerUser);
        
        // Log results
        log.info("========== PERFORMANCE TEST RESULTS ==========");
        log.info("Total requests: {}", concurrentUsers * requestsPerUser);
        log.info("Duration: {}ms", totalDuration);
        log.info("Throughput: {:.2f} req/s", throughput);
        log.info("P50 latency: {}ms", p50);
        log.info("P95 latency: {}ms", p95);
        log.info("P99 latency: {}ms", p99);
        log.info("Error rate: {:.2f}%", errorRate);
        log.info("==============================================");
        
        // Assertions (SLOs)
        assertThat(p95).isLessThan(500);   // P95 < 500ms
        assertThat(errorRate).isLessThan(0.1);  // < 0.1% errors
    }
}
```

### Continuous Performance Testing (CI/CD)
```yaml
# .github/workflows/performance-test.yml
name: Performance Test
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:

jobs:
  performance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start services
        run: docker-compose up -d
      
      - name: Wait for services
        run: sleep 30
      
      - name: Run K6 load test
        uses: grafana/k6-action@v0.3.0
        with:
          filename: k6/eventmaster-load-test.js
      
      - name: Check SLO compliance
        run: |
          if [ $(jq '.metrics.http_req_duration."p(95)"' summary.json) -gt 500 ]; then
            echo "P95 latency SLO violated!"
            exit 1
          fi
```

### Best Practices
1. **Baseline First:** Measure current performance before optimization
2. **Realistic Data:** Use production-like data volume
3. **Monitor Resources:** CPU, Memory, Disk, Network during tests
4. **Test in Staging:** Never run load tests in production!
5. **Gradual Ramp-Up:** Don't slam 1000 users instantly
6. **CI/CD Integration:** Run nightly performance tests

---

# 🎓 Podsumowanie Ścieżki Nauki

Gratulacje! Ukończyłeś wszystkie 50 tematów! 🎉

## ✅ Co Osiągnąłeś?

### Wiedza Techniczna
- ✅ Message-Driven & Event-Driven Architecture
- ✅ CQRS & Event Sourcing od podstaw
- ✅ Apache Kafka & PostgreSQL 18 w praktyce
- ✅ Testowanie systemów rozproszonych
- ✅ Monitoring, Deployment & Scaling

### Umiejętności Praktyczne
- ✅ Implementacja EventMaster od zera
- ✅ Debugging złożonych problemów
- ✅ Testowanie asynchronicznych flowów
- ✅ Performance testing & optimization
- ✅ Production-ready deployment strategies

### Wartość Zawodowa
- 💰 **Senior QA/Test Engineer** - rozumienie architektury distributed systems
- 💰 **Test Architect** - projektowanie test strategies
- 💰 **Backend Developer** - full-stack knowledge (frontend + backend + infrastructure)
- 💰 **DevOps Engineer** - deployment, monitoring, scaling expertise

## 📚 Następne Kroki

1. **Praktyka:** Zaimplementuj wszystkie patterny w EventMaster
2. **Certyfikacja:** Rozważ Confluent Kafka Certification
3. **Contribute:** Open-source projects używające CQRS/Event Sourcing
4. **Naucz Innych:** Najlepszy sposób na utrwalenie wiedzy

## 🔗 Dalsze Zasoby

**Książki:**
- "Implementing Domain-Driven Design" - Vaughn Vernon
- "Building Event-Driven Microservices" - Adam Bellemare
- "Designing Data-Intensive Applications" - Martin Kleppmann

**Kursy:**
- Confluent Kafka Fundamentals
- DDD & Event Sourcing (EventStore Academy)
- Microservices Patterns (Chris Richardson)

**Społeczność:**
- DDD/CQRS/Event Sourcing Slack
- EventMaster GitHub Discussions
- Kafka Users Meetup

---

**Dokument zaktualizowany:** 2025-01-20  
**Wersja:** 2.0 (Complete 50 Topics)  
**Autor:** EventMaster Architecture Team  
**Stack:** PostgreSQL 18, Apache Kafka, Spring Boot 3, Nuxt 3

**Powodzenia w dalszej nauce i rozwijaniu EventMaster! 🚀**

