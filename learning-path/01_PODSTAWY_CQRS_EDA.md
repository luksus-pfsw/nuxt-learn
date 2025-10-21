# Moduł 1: Podstawy CQRS i Event-Driven Architecture

**Czas nauki:** 2-3 godziny  
**Poziom:** Początkujący  
**Cel:** Zrozumienie fundamentów architektury EventMaster

---

## Wprowadzenie

Ten moduł wyjaśnia najważniejsze koncepcje, na których zbudowany jest system EventMaster. Jako tester automatyczny, musisz rozumieć **CO** testujemy i **DLACZEGO** system działa właśnie tak.

---

## 1. Co to jest CQRS?

### 1.1 Definicja dla Początkujących

**CQRS** = Command Query Responsibility Segregation (Podział Odpowiedzialności Komend i Zapytań)

To wzorzec architektoniczny, który **rozdziela** operacje zapisu (komendy) od operacji odczytu (zapytania).

### 1.2 Problem, Który Rozwiązuje CQRS

**Tradycyjny Model (CRUD):**
```
                    [Ta sama baza danych]
                           |
        +------------------+------------------+
        |                                     |
    [CREATE/UPDATE]                      [READ]
    (Zapis - skomplikowany)      (Odczyt - prosty)
```

**Problem:**  
- Modele danych optymalizowane pod zapis są ZŁE do odczytu
- Modele danych optymalizowane pod odczyt są ZŁE do zapisu

**Przykład praktyczny:**

Załóżmy, że mamy wydarzenie (Event) w bazie:
```sql
-- Tabela zoptymalizowana pod zapis (normalizacja 3NF)
Table: events
- id
- title
- organizer_id  -- foreign key do tabeli users

Table: users
- id
- name
- email
```

Kiedy użytkownik chce zobaczyć listę wydarzeń z nazwami organizatorów, musimy wykonać **JOIN**:
```sql
SELECT e.title, u.name as organizer_name
FROM events e
JOIN users u ON e.organizer_id = u.id
```

To jest **wolne** dla tysięcy rekordów!

**Rozwiązanie CQRS:**
```
Tabela WRITE (zoptymalizowana pod zapis):
Table: events
- id
- title
- organizer_id

Tabela READ (zdenormalizowana - szybka!):
Table: event_view
- id
- title  
- organizer_name  <-- DUPLIKAT! Ale szybko się czyta!
- organizer_email
```

Teraz zapytanie o listę wydarzeń to:
```sql
SELECT * FROM event_view  -- Brak JOIN! Błyskawiczne!
```

### 1.3 Przykład Praktyczny #1: EventMaster - Tworzenie Wydarzenia

**Ścieżka WRITE (Command):**
1. Użytkownik wypełnia formularz "Utwórz wydarzenie"
2. Frontend wysyła `POST /api/v1/events`
3. Spring Boot tworzy `CreateEventCommand`
4. Komenda trafia na topik Kafki: `commands.events.create`
5. `EventCommandHandler` odbiera komendę
6. Handler zapisuje `Event` do tabeli `events` (Write Model)
7. Handler emituje `EventCreatedEvent` na topik `events.lifecycle`
8. `EventViewProjector` odbiera event
9. Projector tworzy rekord w tabeli `event_view` (Read Model)

**Ścieżka READ (Query):**
1. Użytkownik wchodzi na `/events`
2. Frontend wysyła `GET /api/v1/events`
3. Spring Boot odpytuje tabelę `event_view`
4. Zwraca listę wydarzeń (bez JOINów!)

### 1.4 Przykład Praktyczny #2: System E-commerce

**Scenariusz:** Użytkownik składa zamówienie

**WRITE:**
```java
// Komenda
CreateOrderCommand {
    orderId: UUID
    userId: UUID
    items: List<Item>
    total: BigDecimal
}

// Handler zapisuje do:
Table: orders (zapis)
Table: order_items (zapis)
Table: inventory (aktualizacja stanów magazynowych)
```

**READ:**
```sql
-- Tabela zdenormalizowana dla szybkiego odczytu
Table: order_summary_view
- order_id
- user_name
- user_email
- total_amount
- items_count
- status
- created_at
```

Zamiast wykonywać 3 JOIN y przy każdym zapytaniu o zamówienia, mamy wszystko w jednej tabeli!

---

## 2. Co to jest Event-Driven Architecture (EDA)?

### 2.1 Definicja

**Event-Driven Architecture** = architektura sterowana zdarzeniami.

System komunikuje się przez **wydarzenia** (events), a nie przez bezpośrednie wywołania funkcji.

### 2.2 Różnica: Synchroniczne vs Asynchroniczne

**Synchroniczne (tradycyjne):**
```
Frontend -> Backend -> Database -> Backend -> Frontend
           (czeka...)(czeka...)(czeka...)
Użytkownik czeka na WSZYSTKO!
```

**Asynchroniczne (EDA):**
```
Frontend -> Backend: "OK, przyjąłem!" (202 Accepted)
            (natychmiast wraca)

W tle:
Backend -> Kafka -> Worker1 -> Worker2 -> ...
```

Użytkownik dostaje natychmiastową odpowiedź!

### 2.3 Przykład Praktyczny #1: Rejestracja Użytkownika

**Tradycyjnie (synchroniczne):**
```java
public void registerUser(UserData data) {
    user = userRepository.save(data);        // 100ms
    emailService.sendWelcomeEmail(user);     // 3000ms (!)
    analyticsService.track Event(user);       // 500ms
    // Użytkownik czeka 3.6 sekundy!
}
```

**Z EDA (asynchroniczne):**
```java
public void registerUser(UserData data) {
    user = userRepository.save(data);                // 100ms
    eventBus.publish(new UserRegisteredEvent(user)); // 1ms
    return "OK!";  // Użytkownik czeka 101ms!
}

// Gdzieś indziej, w tle:
@EventListener
void onUserRegistered(UserRegisteredEvent event) {
    emailService.sendWelcomeEmail(event.user);
}

@EventListener
void onUserRegisteredAnalytics(UserRegisteredEvent event) {
    analyticsService.trackEvent(event.user);
}
```

### 2.4 Przykład Praktyczny #2: EventMaster - Flash Sale Biletów

**Scenariusz:** 100,000 osób próbuje kupić 10,000 biletów o tej samej sekundzie

**Tradycyjnie:**
```
100,000 żądań -> Baza Danych (💥 BOOM! Crash!)
```

**Z EDA + Kafka:**
```
100,000 żądań -> Kafka (może obsłużyć miliony/s)
                   ↓
                Worker1 przetwarza 100 na sekundę
                Worker2 przetwarza 100 na sekundę  
                ... (możemy dodać więcej workerów!)
```

System NIE crashuje! Przetwarza w kolejności (FIFO).

---

## 3. Kluczowe Komponenty w EventMaster

### 3.1 Command (Komenda)

**Co to jest?**  
Komenda to **intencja zmiany** stanu systemu. To jak "rozkaz" - "Zrób to!".

**Cechy:**
- Immutable (niezmienne po utworzeniu)
- Ma nazwę w trybie rozkazującym: `CreateEvent`, `CancelEvent`, `UpdateEvent`
- Może być **odrzucona** (np. walidacja nie przeszła)

**Przykład z kodu EventMaster:**
```java
// backend/src/main/java/com/eventmaster/backend/events/command/CreateEventCommand.java
public record CreateEventCommand(
    @NotNull UUID eventId,           // ID wydarzenia
    @NotNull String organizerId,     // Kto tworzy
    @NotBlank String title,          // Tytuł
    String description,              // Opis (opcjonalny)
    @NotNull Instant eventDate       // Data wydarzenia
) {}
```

**Gdzie używamy:**
1. `EventCommandController` tworzy komendę z żądania HTTP
2. Wysyła ją na topik Kafki `commands.events.create`
3. `EventCommandHandler` odbiera i przetwarza

### 3.2 Event (Zdarzenie)

**Co to jest?**  
Event to **fakt**, że coś się wydarzyło. To jak "raport" - "To się stało!".

**Cechy:**
- Immutable
- Nazwa w czasie przeszłym: `EventCreated`, `EventCancelled`, `EventUpdated`
- NIE może być odrzucone (coś już się stało!)

**Przykład z kodu:**
```java
// backend/src/main/java/com/eventmaster/backend/events/domain/EventCreatedEvent.java
public record EventCreatedEvent(
    UUID eventId,
    String title,
    String description,
    Instant eventDate,
    String organizerId
) {}
```

**Gdzie używamy:**
1. `EventCommandHandler` emituje event po zapisie do bazy Write
2. Wysyła na topik `events.lifecycle`
3. `EventViewProjector` odbiera i aktualizuje Read Model

### 3.3 Aggregate (Agregat)

**Co to jest?**  
Agregat to "główny obiekt biznesowy" w systemie. To encja, wokół której kręci się logika.

**W EventMaster:**
```java
// backend/src/main/java/com/eventmaster/backend/events/domain/Event.java
@Entity
@Table(name = "events")
public class Event {
    @Id
    private UUID id;
    private String title;
    private String description;
    private Instant eventDate;
    private String organizerId;
    
    // To jest agregat! Wszystkie operacje na wydarzeniu
    // przechodzą przez tę klasę
}
```

**Reguły agregatu:**
- Ma unikalny ID (`UUID`)
- Zawiera logikę biznesową
- Jest "atomowy" - albo cały zapisujemy, albo wcale
- Inne obiekty odwołują się do niego przez ID, nie przez referencję

### 3.4 Projector (Projektor)

**Co to jest?**  
Projektor to komponent, który **nasłuchuje eventów** i **aktualizuje Read Model**.

**Przykład z EventMaster:**
```java
// backend/src/main/java/com/eventmaster/backend/events/query/projector/EventViewProjector.java
@Service
@RequiredArgsConstructor
public class EventViewProjector {

    private final EventViewRepository eventViewRepository;

    @KafkaListener(
        topics = "events.lifecycle",
        groupId = "eventmaster-projectors"
    )
    public void projectEventCreated(EventCreatedEvent event) {
        // Tworzy rekord w tabeli read model
        EventView view = new EventView();
        view.setEventId(event.eventId());
        view.setTitle(event.title());
        view.setDescription(event.description());
        view.setEventDate(event.eventDate());
        view.setOrganizerId(event.organizerId());
        
        eventViewRepository.save(view);
    }
}
```

**Dlaczego to ważne?**  
Dzięki projektorom możemy mieć WIELE różnych Read Modelów dla tych samych danych!

Przykład:
- `EventListProjector` → tabela `event_list_view` (lekka, tylko tytuł i data)
- `EventDetailsProjector` → tabela `event_details_view` (pełne dane + JOIN z organizatorem)
- `EventSearchProjector` → Elasticsearch index (dla szybkiego wyszukiwania)

---

## 4. Apache Kafka - Magistrala Zdarzeń

### 4.1 Co to jest Kafka?

**Apache Kafka** to rozproszona platforma do streamingu zdarzeń, pozwalająca na przesyłanie milionów wiadomości na sekundę między aplikacjami.

EventMaster używa Apache Kafka jako głównej magistrali komunikacyjnej między mikroserw isami.

### 4.2 Kluczowe Pojęcia

**Topic (Topik):**
Jak "kanał" w komunikatorze. Producenci publikują wiadomości, konsumenci je czytają.

W EventMaster mamy:
- `commands.events.create` - topik dla komend tworzenia wydarzeń
- `events.lifecycle` - topik dla eventów o cyklu życia wydarzeń

**Producer (Producent):**
Aplikacja, która wysyła wiadomości na topik.

W EventMaster:
- `EventCommandController` jest producentem (wysyła komendy)
- `EventCommandHandler` jest producentem (wysyła eventy)

**Consumer (Konsument):**
Aplikacja, która czyta wiadomości z topiku.

W EventMaster:
- `EventCommandHandler` jest konsumentem (czyta komendy)
- `EventViewProjector` jest konsumentem (czyta eventy)

### 4.3 Przykład Praktyczny: Śledzenie Wiadomości

```
Topik: commands.events.create
=====================================
[Partition 0]
  ├─ Message 1: CreateEventCommand { eventId: abc-123, title: "Koncert" }
  ├─ Message 2: CreateEventCommand { eventId: def-456, title: "Konferencja" }
  └─ Message 3: CreateEventCommand { eventId: ghi-789, title: "Warsztat" }

Konsument: EventCommandHandler (groupId: "eventmaster-handlers")
↓
Przeczytał: Message 1, 2, 3
Offset: 3 (ostatnia przeczytana wiadomość)
```

**Korzyści:**
- Jeśli EventCommandHandler crashuje, po restarcie zaczyna od ostatniego offsetu
- Wiadomości są trwałe - nie znikają po przeczytaniu
- Możemy "przewinąć" i przetworzyć je ponownie

---

## 5. Eventual Consistency (Spójność Ostateczna)

### 5.1 Problem

W CQRS Write Model i Read Model są ODDZIELNE. Między zapisem a zaktualizowaniem odczytu jest **opóźnienie**.

```
T=0s:  User klikawkła "Utwórz wydarzenie"
T=0.1s: Event zapisany w Write Model (tabela `events`)
T=0.2s: Odpowiedź do użytkownika: "OK, przyjęte!" (202 Accepted)
T=0.5s: Event pojawia się w Kafka
T=0.6s: Projektor zapisuje do Read Model (tabela `event_view`)
```

**Pytanie:** Co jeśli użytkownik od razu po kliknięciu "Utwórz" wejdzie na listę?  
**Odpowiedź:** Nie zobaczy swojego wydarzenia przez ~400ms!

### 5.2 Rozwiązania

**1. Optymistic UI (Optymistyczny Interfejs):**
Frontend "udaje", że event już istnieje i pokazuje go na liście lokalnie.

```vue
// frontend/pages/events/create.vue
const onSubmit = async (formData) => {
    // Dodaj lokalnie
    localEvents.value.push(formData);
    
    // Wyślij do API
    await $fetch('/api/v1/events', {
        method: 'POST',
        body: formData
    });
    
    // Przekieruj
    router.push('/events');
};
```

**2. Polling (Odpytywanie):**
Frontend co sekundę odpytuje API, czy event się pojawił.

**3. WebSocket/SSE (Server-Sent Events):**
Backend powiadamia frontend, kiedy event jest gotowy.

---

## 6. Zadania Praktyczne dla Testera

### Zadanie 1: Śledzenie Komendy
**Cel:** Zrozumieć przepływ komendy przez system.

1. Uruchom aplikację: `docker-compose up`
2. Zaloguj się do Kafka UI: `http://localhost:8080` (kafka-ui)
3. Obserwuj topik `commands.events.create`
4. W przeglądarce utwórz nowe wydarzenie
5. Sprawdź, czy komenda pojawiła się w Kafce

**Pytania:**
- Jaki jest format wiadomości?
- Ile czasu minęło między wysłaniem a pojawieniem się w Kafce?

### Zadanie 2: Test Eventual Consistency
**Cel:** Zmierzyć opóźnienie między Write a Read Model.

1. Napisz test Playwright:
```typescript
test('eventual consistency delay', async ({ page }) => {
    const startTime = Date.now();
    
    // Utwórz event
    await page.goto('/events/create');
    await page.fill('[name="title"]', 'Test Event');
    await page.click('[type="submit"]');
    
    // Czekaj, aż pojawi się na liście
    await page.goto('/events');
    await page.waitForSelector('text=Test Event');
    
    const endTime = Date.now();
    const delay = endTime - startTime;
    
    console.log(`Eventual consistency delay: ${delay}ms`);
    expect(delay).toBeLessThan(2000); // Max 2 sekundy
});
```

### Zadanie 3: Test Idempotencji
**Cel:** Sprawdzić, czy wielokrotne wysłanie tej samej komendy nie powoduje duplikatów.

```java
@Test
void shouldNotCreateDuplicateEvents() {
    UUID eventId = UUID.randomUUID();
    CreateEventCommand cmd = new CreateEventCommand(
        eventId, "org1", "Test", "Desc", Instant.now()
    );
    
    // Wyślij tę samą komendę dwukrotnie
    kafkaTemplate.send("commands.events.create", cmd);
    kafkaTemplate.send("commands.events.create", cmd);
    
    // Czekaj
    await().atMost(5, SECONDS).untilAsserted(() -> {
        List<Event> events = eventRepository.findAll();
        // Powinien być tylko JEDEN event!
        assertThat(events).hasSize(1);
    });
}
```

---

## 7. Podsumowanie

**Co nauczyliśmy się:**
1. CQRS rozdziela zapis (Command) i odczyt (Query)
2. Event-Driven Architecture komunikuje się przez wydarzenia
3. Apache Kafka to rozproszona magistrala zdarzeń
4. Eventual Consistency oznacza krótkie opóźnienie między Write a Read
5. Aggregate to główny obiekt biznesowy
6. Projector aktualizuje Read Model na podstawie eventów

**Kluczowe terminy:**
- **Command** - rozkaz zmiany (CreateEvent, UpdateEvent)
- **Event** - fakt, że coś się stało (EventCreated, EventUpdated)
- **Aggregate** - główny obiekt biznesowy (Event entity)
- **Projector** - aktualizuje Read Model
- **Topic** - kanał komunikacji w Kafce
- **Eventual Consistency** - dane są spójne "za chwilę", nie natychmiast

**Następny moduł:** [02_SPRING_BOOT_BACKEND.md](./02_SPRING_BOOT_BACKEND.md)

---

**Pytania do samodzielnego przemyślenia:**

1. Dlaczego nie możemy po prostu używać JOINów zamiast duplikować dane?
2. Co się stanie, jeśli Kafka będzie niedostępna przez 1 godzinę?
3. Czy Read Model może być "przed" Write Model? (Czy da się "cofnąć czas"?)
4. Jak testować system asynchroniczny? (Odpowiedź: Awaitility, Testcontainers!)

