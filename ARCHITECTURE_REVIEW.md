# EventMaster - Przegląd Architektury CQRS

**Data przeglądu:** 2025-10-19  
**Wersja:** 0.0.1-SNAPSHOT  
**Recenzent:** Architekt CQRS/Java/Spring/Nuxt

---

## 1. Executive Summary

Projekt EventMaster to aplikacja do zarządzania wydarzeniami zbudowana w oparciu o architekturę **CQRS (Command Query Responsibility Segregation)** z wykorzystaniem Kafki jako message brokera. Backend jest zaimplementowany w **Spring Boot 3.3.1** z **Java 21**, frontend w **Nuxt 4** z **Vue 3**. Projekt wykorzystuje **PostgreSQL** jako Write Model, **CockroachDB** jako Read Model oraz **Keycloak** do autoryzacji OAuth2/OIDC.

### Status ogólny: ✅ **DOBRY FUNDAMENT** z kilkoma obszarami do poprawy

---

## 2. Analiza Architektury CQRS

### 2.1 ✅ Mocne strony implementacji

#### 2.1.1 Separacja Write i Read Model
- **Doskonała separacja na poziomie konfiguracji JPA**
  - Oddzielne DataSource dla zapisu (`WriteDataSourceConfig`) i odczytu (`ReadDataSourceConfig`)
  - Dedykowane EntityManagerFactory dla każdego modelu
  - Oddzielne TransactionManager'y
  - Prawidłowa izolacja pakietów:
    - Write: `com.eventmaster.backend.events.domain` → PostgreSQL
    - Read: `com.eventmaster.backend.events.query.model` → CockroachDB

#### 2.1.2 Przepływ komend (Command Flow)
```
Controller → Kafka Topic (commands.events.create) → CommandHandler → Write DB → Domain Event
```
- Asynchroniczne przetwarzanie komend przez Kafkę
- Prawidłowa walidacja na poziomie HTTP (`@Valid`, Bean Validation)
- Użycie immutable records dla komend (`CreateEventCommand`)
- Automatyczne generowanie UUID dla agregatu

#### 2.1.3 Przepływ eventów domenowych (Domain Event Flow)
```
CommandHandler → Kafka Topic (domain.events.lifecycle) → Projector → Read DB
```
- Dedykowany topic dla zdarzeń domenowych
- Projector (`EventViewProjector`) poprawnie transformuje wydarzenia do read model
- Oddzielny consumer group dla projektorów (`eventmaster-projectors-crdb`)

#### 2.1.4 Event-Driven Architecture
- Kafka jako event bus
- Dead Letter Queue (DLQ) przez `DeadLetterPublishingRecoverer`
- Retry mechanism z `FixedBackOff` (1s, 2 próby)

### 2.2 ⚠️ Problemy i braki w implementacji CQRS

#### 2.2.1 🔴 **KRYTYCZNE: Brak Kafki w docker-compose.yml**
**Problem:** W pliku `docker/docker-compose.yml` nie ma Kafki/Redpanda, mimo że aplikacja wymaga message brokera.

**Wpływ:** Aplikacja nie uruchomi się poprawnie bez Kafki.

**Rekomendacja:**
```yaml
redpanda:
  image: docker.redpanda.com/redpandadata/redpanda:latest
  command:
    - redpanda
    - start
    - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
    - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    - --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082
    - --advertise-pandaproxy-addr internal://redpanda:8082,external://localhost:18082
    - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
    - --rpc-addr redpanda:33145
    - --advertise-rpc-addr redpanda:33145
    - --smp 1
    - --memory 1G
    - --mode dev-container
    - --default-log-level=info
  ports:
    - "19092:19092"
    - "18081:18081"
    - "18082:18082"
    - "9644:9644"
```

#### 2.2.2 🟡 **Brak Event Sourcing**
**Problem:** Projekt używa CQRS, ale nie implementuje pełnego Event Sourcingu.

**Obecne podejście:** 
- Agregat `Event` jest zapisywany jako snapshot (stan) w PostgreSQL
- Brak historii zdarzeń (event store)

**Zalety obecnego podejścia:**
- Prostsze w implementacji
- Wystarczające dla wielu przypadków użycia
- Łatwiejsze debugowanie

**Rekomendacja dla rozszerzenia:**
Jeśli potrzebujecie pełnej historii i audytu, dodajcie event store:
```sql
CREATE TABLE event_store (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data JSONB NOT NULL,
    version BIGINT NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    metadata JSONB,
    UNIQUE(aggregate_id, version)
);
```

#### 2.2.3 🟡 **Brak obsługi idempotencji**
**Problem:** Brak mechanizmu zapobiegającego wielokrotnemu przetworzeniu tej samej komendy/eventu.

**Scenariusz problemu:**
1. Kafka retry próbuje ponownie przetworzyć komendę
2. Ten sam event może być zapisany dwukrotnie

**Rekomendacja:**
Dodać tabelę processed_messages i sprawdzać przed przetworzeniem:
```java
@Service
public class IdempotencyService {
    public boolean isProcessed(UUID messageId) {
        return processedMessageRepository.existsById(messageId);
    }
    
    @Transactional
    public void markAsProcessed(UUID messageId) {
        processedMessageRepository.save(new ProcessedMessage(messageId));
    }
}
```

#### 2.2.4 🟡 **Brak saga/process manager**
**Problem:** Projekt nie posiada mechanizmu orkiestracji długotrwałych transakcji biznesowych.

**Kiedy będzie potrzebne:**
- Przy rejestracji uczestnika (rezerwacja miejsca + płatność + email)
- Przy anulowaniu wydarzenia (zwrot pieniędzy + notyfikacje)

**Rekomendacja:**
Rozważyć implementację Saga Pattern (choreography lub orchestration) w przyszłości.

#### 2.2.5 🟡 **Brak snapshottingu dla agregatów**
**Problem:** Przy Event Sourcing (gdy zostanie dodany), odtwarzanie agregatu z setek eventów może być kosztowne.

**Rekomendacja (na przyszłość):**
Implementować snapshotting co N eventów.

#### 2.2.6 🟡 **Brak wersjonowania eventów**
**Problem:** `EventCreatedEvent` nie ma pola version. Przy ewolucji schematu może być problem z backward compatibility.

**Rekomendacja:**
```java
public record EventCreatedEvent(
    UUID eventId,
    String title,
    String description,
    Instant eventDate,
    String organizerId,
    int schemaVersion  // Dodać wersję
) {}
```

#### 2.2.7 🟠 **Eventual Consistency nie jest komunikowane użytkownikowi**
**Problem:** Frontend traktuje odpowiedź `202 Accepted` jak sukces, ale nie informuje użytkownika o eventual consistency.

**Obecne zachowanie:**
```java
return ResponseEntity.accepted().build(); // 202
```

**Rekomendacja:**
1. Zwracać `commandId` w odpowiedzi
2. Frontend może odpytywać o status przez WebSocket/SSE
3. Lub pokazać komunikat "Przetwarzamy Twoje żądanie..."

Przykład:
```java
@PostMapping
public ResponseEntity<CommandResponse> createEvent(...) {
    UUID commandId = UUID.randomUUID();
    var command = new CreateEventCommand(commandId, ...);
    kafkaTemplate.send(TOPIC, commandId.toString(), command);
    return ResponseEntity.accepted()
        .body(new CommandResponse(commandId, "ACCEPTED"));
}
```

---

## 3. Analiza Backend (Spring Boot)

### 3.1 ✅ Mocne strony

#### 3.1.1 Konfiguracja i struktura
- **Spring Boot 3.3.1** z Java 21 (aktualna wersja LTS)
- Dobra separacja warstw (config, domain, api, query)
- Użycie Lombok dla redukcji boilerplate
- Flyway dla migracji (Write Model)

#### 3.1.2 Bezpieczeństwo
- OAuth2 Resource Server z JWT
- Integracja z Keycloak
- CORS prawidłowo skonfigurowany
- Endpoint health bez autentykacji (dobre dla health checks)

#### 3.1.3 Testy
- Testcontainers dla PostgreSQL, Keycloak, Redpanda
- Abstrakcyjna klasa `BaseIntegrationTest` dla reużycia
- Dynamic properties dla testów

### 3.2 ⚠️ Problemy i braki

#### 3.2.1 🟡 **Brak warstwy aplikacyjnej (Application Service)**
**Problem:** `EventCommandHandler` pełni jednocześnie rolę Kafka Listener i Application Service.

**Rekomendacja:**
Rozdzielić odpowiedzialności:
```java
// Application Service (logika biznesowa)
@Service
public class EventApplicationService {
    public void createEvent(CreateEventCommand cmd) {
        Event event = Event.create(cmd);
        eventRepository.save(event);
        return event.getDomainEvents();
    }
}

// Kafka Listener (infrastruktura)
@Component
public class CreateEventCommandListener {
    @KafkaListener(...)
    public void handle(CreateEventCommand cmd) {
        List<DomainEvent> events = eventAppService.createEvent(cmd);
        events.forEach(e -> kafkaTemplate.send(DOMAIN_TOPIC, e));
    }
}
```

#### 3.2.2 🟡 **Brak walidacji biznesowej w agregacie**
**Problem:** Klasa `Event` jest anemic domain model (tylko gettery/settery, brak logiki).

**Rekomendacja:**
```java
@Entity
public class Event {
    private UUID id;
    private String title;
    private Instant eventDate;
    
    // Factory method z walidacją
    public static Event create(UUID id, String title, Instant eventDate) {
        validateTitle(title);
        validateEventDate(eventDate);
        return new Event(id, title, eventDate);
    }
    
    private static void validateTitle(String title) {
        if (title == null || title.isBlank()) {
            throw new InvalidEventException("Title cannot be empty");
        }
    }
    
    private static void validateEventDate(Instant date) {
        if (date.isBefore(Instant.now())) {
            throw new InvalidEventException("Event date must be in future");
        }
    }
}
```

#### 3.2.3 🟠 **Brak obsługi błędów w kontrolerach**
**Problem:** Brak globalnego `@ControllerAdvice` dla spójnej obsługi wyjątków.

**Rekomendacja:**
```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_ERROR", ex.getMessage()));
    }
    
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.internalServerError()
            .body(new ErrorResponse("INTERNAL_ERROR", "Something went wrong"));
    }
}
```

#### 3.2.4 🟡 **Brak struktury dla query objects**
**Problem:** Query side nie ma dedykowanych query objects - kontroler bezpośrednio używa repozytorium.

**Rekomendacja (dla bardziej złożonych query):**
```java
// Query object
public record GetAllEventsQuery(
    Optional<String> organizerId,
    Optional<Instant> fromDate,
    Pageable pageable
) {}

// Query handler
@Service
public class EventQueryHandler {
    public Page<EventViewDTO> handle(GetAllEventsQuery query) {
        // Logika filtrowania
    }
}
```

#### 3.2.5 🟠 **Brak paginacji w Query API**
**Problem:** `GET /api/v1/events` zwraca wszystkie wydarzenia bez paginacji.

**Rekomendacja:**
```java
@GetMapping
public ResponseEntity<Page<EventViewDTO>> getAllEvents(
    @RequestParam(defaultValue = "0") int page,
    @RequestParam(defaultValue = "20") int size
) {
    Pageable pageable = PageRequest.of(page, size);
    return ResponseEntity.ok(eventViewRepository.findAll(pageable));
}
```

#### 3.2.6 🟡 **Brak health check dla Kafka**
**Problem:** Actuator health sprawdza tylko DB, ale nie Kafkę.

**Rekomendacja:**
```properties
management.health.kafka.enabled=true
```

#### 3.2.7 🟠 **Hardcoded topic names**
**Problem:** Topic names są hardcoded jako `public static final String`.

**Rekomendacja:**
```properties
# application.properties
kafka.topics.commands.events.create=commands.events.create
kafka.topics.domain.events.lifecycle=domain.events.lifecycle
```

```java
@ConfigurationProperties(prefix = "kafka.topics")
public record KafkaTopics(
    Commands commands,
    Domain domain
) {
    public record Commands(String eventsCreate) {}
    public record Domain(String eventsLifecycle) {}
}
```

#### 3.2.8 🟡 **Brak monitoringu i metryk**
**Problem:** Brak metryk biznesowych (liczba przetworzonych komend, czas przetwarzania, etc.).

**Rekomendacja:**
Dodać Micrometer metrics:
```java
@Timed(value = "command.processing.time", description = "Time to process command")
@Counted(value = "command.processed", description = "Number of processed commands")
public void handleCreateEventCommand(CreateEventCommand command) {
    // ...
}
```

#### 3.2.9 🟠 **Brak logów strukturalnych**
**Problem:** Logi są tekstowe, trudne do parsowania przez systemy monitorujące.

**Rekomendacja:**
Użyć Logstash Logback Encoder:
```xml
<dependency>
    <groupId>net.logstash.logback</groupId>
    <artifactId>logstash-logback-encoder</artifactId>
    <version>7.4</version>
</dependency>
```

#### 3.2.10 🟡 **Transactional Outbox Pattern nie jest używany**
**Problem:** Zapis do DB i wysłanie do Kafki nie są atomowe. Jeśli Kafka jest down, event się zgubi.

**Rekomendacja:**
Implementować Transactional Outbox:
```sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed BOOLEAN DEFAULT FALSE
);
```

Poller wysyłający eventy z outbox do Kafki.

---

## 4. Analiza Frontend (Nuxt)

### 4.1 ✅ Mocne strony

#### 4.1.1 Technologie
- **Nuxt 4** z Vue 3 (najnowsze wersje)
- **@sidebase/nuxt-auth** dla OAuth2/OIDC
- **TypeScript** (type safety)
- **Playwright** dla testów E2E

#### 4.1.2 Struktura
- Pages-based routing (konwencja Nuxt)
- Oddzielenie strony listowania i tworzenia eventów
- Middleware `auth` dla protected routes

#### 4.1.3 Integracja z backendem
- Fetch przez `/api/v1/events` (proxied przez Caddy)
- Authentication token automatycznie dołączany przez nuxt-auth

### 4.2 ⚠️ Problemy i braki

#### 4.2.1 🔴 **Problem z create.vue - brak zależności PrimeVue**
**Problem:** Strona `/events/create.vue` używa komponentów PrimeVue (InputText, Editor, Calendar, Button, Toast), ale **PrimeVue nie jest w dependencies**.

```vue
import InputText from 'primevue/inputtext';
import Editor from 'primevue/editor';
import Calendar from 'primevue/calendar';
import Button from 'primevue/button';
import { useToast } from 'primevue/usetoast';
```

**Rekomendacja:**
```bash
npm install primevue @primevue/themes
```

I konfiguracja w `nuxt.config.ts`:
```ts
export default defineNuxtConfig({
  modules: ['@sidebase/nuxt-auth', '@primevue/nuxt-module'],
  primevue: {
    options: {
      theme: 'aura'
    }
  }
})
```

#### 4.2.2 🟡 **Brak vee-validate i zod**
**Problem:** `create.vue` importuje `vee-validate` i `zod`, ale nie są w `package.json`.

**Rekomendacja:**
```bash
npm install vee-validate zod @vee-validate/zod
```

#### 4.2.3 🟠 **Brak obsługi błędów w fetch**
**Problem:** W `events/index.vue` błędy są logowane do console, ale nie pokazywane użytkownikowi.

**Rekomendacja:**
```vue
<script setup>
const events = ref([]);
const loading = ref(true);
const error = ref(null);

onMounted(async () => {
  try {
    const response = await fetch('/api/v1/events');
    if (!response.ok) throw new Error('Failed to fetch events');
    events.value = await response.json();
  } catch (e) {
    error.value = e.message;
  } finally {
    loading.value = false;
  }
});
</script>

<template>
  <div v-if="error" class="error">{{ error }}</div>
</template>
```

#### 4.2.4 🟡 **Brak composables dla API calls**
**Problem:** Logika fetch jest duplikowana w komponentach.

**Rekomendacja:**
```ts
// composables/useEvents.ts
export const useEvents = () => {
  const getEvents = async () => {
    return await $fetch('/api/v1/events');
  };
  
  const createEvent = async (data: CreateEventRequest) => {
    return await $fetch('/api/v1/events', {
      method: 'POST',
      body: data
    });
  };
  
  return { getEvents, createEvent };
};
```

#### 4.2.5 🟠 **Brak loading states i optymistycznego UI**
**Problem:** Po submitcie formularza użytkownik nie wie, czy request się powiódł, dopóki nie przejdzie na listę.

**Rekomendacja:**
- Pokazywać spinner podczas request
- Optymistic update (dodać event do listy przed potwierdzeniem)
- Toast notification o sukcesie/błędzie

#### 4.2.6 🟡 **Brak TypeScript types dla API responses**
**Problem:** Brak typów dla odpowiedzi z API.

**Rekomendacja:**
```ts
// types/api.ts
export interface EventViewDTO {
  eventId: string;
  title: string;
  eventDate: string;
}

export interface CreateEventRequest {
  title: string;
  description?: string;
  eventDate: string;
}
```

#### 4.2.7 🟠 **Brak cache dla events**
**Problem:** Każde wejście na `/events` pobiera dane na nowo.

**Rekomendacja:**
Użyć `useAsyncData` z cache:
```vue
const { data: events, refresh } = await useAsyncData(
  'events',
  () => $fetch('/api/v1/events'),
  { 
    lazy: true,
    server: false 
  }
);
```

#### 4.2.8 🟡 **Brak internationalization (i18n)**
**Problem:** Teksty są hardcoded po polsku, co utrudnia międzynarodowe wdrożenie.

**Rekomendacja (opcjonalne):**
```bash
npm install @nuxtjs/i18n
```

---

## 5. Analiza Infrastruktury

### 5.1 ✅ Mocne strony

#### 5.1.1 Docker Compose
- PostgreSQL dla Write Model
- CockroachDB dla Read Model
- Keycloak dla autoryzacji
- Caddy jako reverse proxy

#### 5.1.2 Caddy
- Prawidłowe routowanie:
  - `/api/v1/*` → Spring Boot
  - `/api/auth/*` → Nuxt Server (OIDC callback)
  - `*` → Nuxt Frontend

### 5.2 ⚠️ Problemy i braki

#### 5.2.1 🔴 **BRAK KAFKI/REDPANDA** (już wspomniane wyżej)

#### 5.2.2 🟡 **Brak volume dla Keycloak realm import**
**Problem:** Keycloak ma konfigurację:
```yaml
KEYCLOAK_IMPORT: /opt/keycloak/data/import/eventmaster-realm.json
volumes:
  - ./realm-config:/opt/keycloak/data/import
```

Ale nie widać pliku `realm-config/eventmaster-realm.json`.

**Rekomendacja:**
Sprawdzić, czy plik istnieje, lub usunąć `KEYCLOAK_IMPORT` jeśli realm jest konfigurowany ręcznie.

#### 5.2.3 🟠 **Brak health checks w docker-compose**
**Rekomendacja:**
```yaml
postgres:
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U user"]
    interval: 10s
    timeout: 5s
    retries: 5

keycloak:
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
    interval: 10s
    timeout: 5s
    retries: 5
```

#### 5.2.4 🟡 **Brak docker image dla backend/frontend**
**Problem:** Brak Dockerfile dla backend i frontend.

**Rekomendacja:**
```dockerfile
# Backend Dockerfile
FROM eclipse-temurin:21-jre-alpine
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]

# Frontend Dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
CMD ["node", ".output/server/index.mjs"]
```

#### 5.2.5 🟠 **Brak .env file dla secrets**
**Problem:** Hasła są hardcoded w docker-compose.yml.

**Rekomendacja:**
```yaml
postgres:
  environment:
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
```

#### 5.2.6 🟡 **CockroachDB używa auto-schema (hibernate.hbm2ddl.auto=update)**
**Problem:** Read Model używa Hibernate auto-update zamiast Flyway.

**Rekomendacja:**
Dodać Flyway dla Read Model lub przynajmniej `validate` zamiast `update` na produkcji.

---

## 6. Testowanie

### 6.1 ✅ Obecne
- `BaseIntegrationTest` z Testcontainers
- PostgreSQL, Keycloak, Redpanda w testach

### 6.2 ⚠️ Braki

#### 6.2.1 🟡 **Brak testów jednostkowych dla logiki domenowej**
**Rekomendacja:**
```java
@Test
void shouldNotCreateEventWithPastDate() {
    assertThatThrownBy(() -> Event.create(UUID.randomUUID(), "Title", Instant.now().minus(1, ChronoUnit.DAYS)))
        .isInstanceOf(InvalidEventException.class);
}
```

#### 6.2.2 🟡 **Brak testów dla projektorów**
**Rekomendacja:**
Test, że `EventViewProjector` poprawnie tworzy read model z domain event.

#### 6.2.3 🟡 **Brak testów dla eventual consistency**
**Rekomendacja:**
Test end-to-end:
1. Wyślij komendę
2. Poczekaj na przetworzenie
3. Sprawdź, czy event jest w read model

```java
@Test
void shouldEventuallyAppearInReadModel() {
    // Given
    var cmd = new CreateEventCommand(...);
    
    // When
    kafkaTemplate.send(TOPIC, cmd);
    
    // Then
    await().atMost(5, SECONDS).untilAsserted(() -> {
        Optional<EventView> view = eventViewRepository.findById(cmd.eventId());
        assertThat(view).isPresent();
    });
}
```

#### 6.2.4 🟠 **Brak testów E2E dla frontend**
**Problem:** Playwright jest w dependencies, ale brak testów.

**Rekomendacja:**
```ts
// tests/events.spec.ts
test('should create event', async ({ page }) => {
  await page.goto('/events/create');
  await page.fill('[name="title"]', 'Test Event');
  await page.click('button[type="submit"]');
  await expect(page).toHaveURL('/events');
});
```

---

## 7. Bezpieczeństwo

### 7.1 ✅ Mocne strony
- OAuth2/OIDC z Keycloak
- JWT validation na backend
- CORS skonfigurowany
- Endpoint `/api/v1/events` POST wymaga autentykacji

### 7.2 ⚠️ Problemy

#### 7.2.1 🟠 **Brak rate limiting**
**Rekomendacja:**
Użyć Bucket4j lub Spring Cloud Gateway dla rate limiting.

#### 7.2.2 🟡 **Brak autoryzacji na poziomie zasobu**
**Problem:** Każdy zalogowany użytkownik może tworzyć eventy, ale nie ma walidacji, czy może je modyfikować.

**Rekomendacja:**
```java
@PreAuthorize("@eventSecurity.canModify(#eventId, authentication)")
@PutMapping("/{eventId}")
public ResponseEntity<?> updateEvent(@PathVariable UUID eventId, ...) {
    // ...
}
```

#### 7.2.3 🟡 **Sekret auth hardcoded**
**Problem:**
```ts
secret: process.env.NUXT_AUTH_SECRET || 'super-tajny-sekret-zmien-mnie',
```

**Rekomendacja:**
Wymagać `NUXT_AUTH_SECRET` i rzucać błąd, jeśli nie ma.

#### 7.2.4 🟠 **Brak HTTPS w lokalnym dev**
**Problem:** Caddy używa HTTP, a nie HTTPS w dev.

**Rekomendacja (opcjonalne):**
Użyć Caddy automatic HTTPS z self-signed cert.

---

## 8. Dokumentacja

### 8.1 ⚠️ Braki

#### 8.1.1 🟡 **Brak README.md**
**Rekomendacja:**
Dodać README z:
- Opisem projektu
- Architekturą (diagram CQRS flow)
- Instrukcją uruchomienia
- Lista endpointów API

#### 8.1.2 🟡 **Brak OpenAPI/Swagger**
**Rekomendacja:**
```xml
<dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>2.3.0</version>
</dependency>
```

Dostępne pod: `http://localhost:8080/swagger-ui.html`

#### 8.1.3 🟠 **Brak ADR (Architecture Decision Records)**
**Rekomendacja:**
Dokumentować kluczowe decyzje architektoniczne:
- Dlaczego CQRS?
- Dlaczego Kafka?
- Dlaczego CockroachDB dla read model?

---

## 9. Wydajność

### 9.1 ⚠️ Potencjalne problemy

#### 9.1.1 🟡 **Brak indexów w read model**
**Rekomendacja:**
```sql
CREATE INDEX idx_event_views_organizer ON event_views(organizer_id);
CREATE INDEX idx_event_views_date ON event_views(event_date);
```

#### 9.1.2 🟡 **Brak cache dla często odpytywanych danych**
**Rekomendacja:**
Użyć Redis lub Caffeine cache dla hot data.

#### 9.1.3 🟠 **Brak connection pooling config**
**Rekomendacja:**
```properties
spring.datasource.write.hikari.maximum-pool-size=10
spring.datasource.write.hikari.minimum-idle=5
spring.datasource.write.hikari.connection-timeout=20000
```

---

## 10. Monitoring i Observability

### 10.1 ⚠️ Braki

#### 10.1.1 🟡 **Brak distributed tracing**
**Rekomendacja:**
Dodać Spring Cloud Sleuth + Zipkin/Jaeger:
```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
```

#### 10.1.2 🟡 **Brak Prometheus metrics**
**Rekomendacja:**
```properties
management.endpoints.web.exposure.include=health,prometheus
management.metrics.export.prometheus.enabled=true
```

#### 10.1.3 🟠 **Brak log aggregation**
**Rekomendacja:**
Użyć ELK stack (Elasticsearch, Logstash, Kibana) lub Loki.

---

## 11. Brakujące funkcjonalności CQRS

### 11.1 🟡 **Command Bus**
**Obecne:** Komendy są bezpośrednio wysyłane do Kafki.

**Rekomendacja (opcjonalne):**
Implementować Command Bus dla lokalnego routingu:
```java
@Component
public class CommandBus {
    private final Map<Class<?>, CommandHandler<?>> handlers = new HashMap<>();
    
    public <C> void dispatch(C command) {
        CommandHandler<C> handler = (CommandHandler<C>) handlers.get(command.getClass());
        handler.handle(command);
    }
}
```

### 11.2 🟡 **Query Bus**
**Obecne:** Kontrolery bezpośrednio używają repozytoriów.

**Rekomendacja (opcjonalne):**
Podobnie jak Command Bus, ale dla query.

### 11.3 🟡 **Domain Events w agregacie**
**Obecne:** Domain events są tworzone w CommandHandler.

**Rekomendacja:**
Agregat powinien produkować domain events:
```java
@Entity
public class Event {
    @Transient
    private List<DomainEvent> domainEvents = new ArrayList<>();
    
    public static Event create(...) {
        Event event = new Event(...);
        event.addDomainEvent(new EventCreatedEvent(...));
        return event;
    }
    
    public List<DomainEvent> getDomainEvents() {
        return List.copyOf(domainEvents);
    }
}
```

---

## 12. Rekomendacje priorytetowe

### 🔴 **KRYTYCZNE (do naprawienia natychmiast):**
1. **Dodać Kafka/Redpanda do docker-compose.yml**
2. **Dodać brakujące dependencies do frontend (PrimeVue, vee-validate, zod)**

### 🟡 **WYSOKIE (do zrobienia w najbliższym czasie):**
3. Implementować idempotencję przetwarzania komend/eventów
4. Dodać Transactional Outbox Pattern
5. Dodać obsługę błędów (@ControllerAdvice)
6. Dodać paginację do query API
7. Dodać README.md z dokumentacją
8. Dodać testy jednostkowe dla domeny

### 🟠 **ŚREDNIE (do rozważenia):**
9. Rozdzielić Application Service od Kafka Listener
10. Przenieść logikę walidacji do agregatu (Rich Domain Model)
11. Dodać health checks dla Kafka
12. Dodać OpenAPI/Swagger
13. Dodać composables dla API calls w frontend
14. Dodać strukturalne logi (JSON)
15. Dodać metryki (Micrometer)

### 🔵 **NISKIE (nice to have):**
16. Implementować Event Sourcing (jeśli potrzebny audit trail)
17. Dodać Saga Pattern (gdy będą długotrwałe transakcje)
18. Dodać Command/Query Bus
19. Dodać distributed tracing
20. Dodać i18n
21. Dodać cache (Redis)

---

## 13. Podsumowanie

### ✅ Co jest zrobione dobrze:
- **Solidny fundament CQRS** z separacją write/read model
- **Event-Driven Architecture** z Kafką
- **Nowoczesny stack** (Spring Boot 3, Java 21, Nuxt 4, Vue 3)
- **Bezpieczeństwo** (OAuth2/OIDC)
- **Testcontainers** dla testów integracyjnych

### ⚠️ Co wymaga poprawy:
- **Brak Kafki w docker-compose** (krytyczne)
- **Brak dependencies w frontend** (krytyczne)
- **Brak idempotencji i transactional outbox**
- **Anemic domain model**
- **Brak obsługi eventual consistency w UI**
- **Brak dokumentacji**

### 🎯 Następne kroki:
1. Naprawić krytyczne problemy (Kafka, dependencies)
2. Dodać idempotencję i outbox
3. Napisać testy
4. Dodać dokumentację (README, OpenAPI)
5. Rozważyć rozszerzenie o Event Sourcing i Saga Pattern w przyszłości

---

**Ocena ogólna:** 7/10

Projekt ma bardzo dobry fundament architektoniczny i używa najlepszych praktyk CQRS. Główne problemy to brak Kafki w docker-compose, brakujące dependencies we frontend oraz kilka luk w implementacji (idempotencja, outbox pattern). Po naprawieniu krytycznych problemów, projekt będzie gotowy do dalszego rozwoju.

---

**Autor:** Architekt CQRS/Java/Spring/Nuxt  
**Data:** 2025-10-19
