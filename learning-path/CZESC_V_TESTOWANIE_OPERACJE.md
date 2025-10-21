# 🎓 CZĘŚĆ V: TESTOWANIE & OPERACJE (Tematy 41-50)

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4 + Nuxt 3  
**Cel:** Praktyczne testowanie i operacje systemów rozproszonych  
**Czas nauki:** 3-4 tygodnie  
**Wymagania:** Ukończone CZĘŚĆ I-IV

---

## 📋 Spis Treści - CZĘŚĆ V

41. [Integration Testing with Testcontainers](#temat-41-integration-testing-testcontainers)
42. [Contract Testing with Pact](#temat-42-contract-testing-pact)
43. [Chaos Engineering](#temat-43-chaos-engineering)
44. [Performance Testing](#temat-44-performance-testing)
45. [Monitoring & Observability](#temat-45-monitoring-observability)
46. [Distributed Tracing](#temat-46-distributed-tracing)
47. [Health Checks & Readiness](#temat-47-health-checks-readiness)
48. [Blue-Green & Canary Deployment](#temat-48-blue-green-canary)
49. [Database Migrations](#temat-49-database-migrations)
50. [Production Checklist](#temat-50-production-checklist)

---

## 🎯 Cele Nauki - CZĘŚĆ V

Po ukończeniu Części V będziesz:
- ✅ Pisać testy integracyjne z Testcontainers
- ✅ Implementować Contract Testing
- ✅ Prowadzić Chaos Engineering
- ✅ Testować wydajność systemów rozproszonych
- ✅ Konfigurować monitoring i observability
- ✅ Implementować distributed tracing
- ✅ Projektować health checks
- ✅ Wdrażać bez downtime
- ✅ Zarządzać migracjami baz danych
- ✅ Przygotować system do produkcji

---

## Temat 41: Integration Testing with Testcontainers

### 41.1 Definicja

**Testcontainers** to biblioteka Java pozwalająca uruchamiać rzeczywiste kontenery Docker w testach integracyjnych. Zamiast mocków, testujesz z **prawdziwymi** bazami danych, Kafka, Redis itd.

### 41.2 Dlaczego Testcontainers?

**Problem z mockami:**
```java
// ❌ BAD: Mock nie wykryje prawdziwych problemów
@Mock
private KafkaTemplate<String, Event> kafkaTemplate;

@Test
void shouldPublishEvent() {
    service.publishEvent(event);
    verify(kafkaTemplate).send("events", event);  // OK, ale czy Kafka naprawdę działa?
}
```

**Rozwiązanie Testcontainers:**
```java
// ✅ GOOD: Prawdziwa Kafka!
@Testcontainers
class EventPublisherIntegrationTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );
    
    @Test
    void shouldPublishAndConsumeEvent() {
        // Given: Prawdziwa Kafka działa
        service.publishEvent(event);
        
        // Then: Konsumuj z prawdziwej Kafka
        var records = consumer.poll(Duration.ofSeconds(5));
        assertThat(records).hasSize(1);
    }
}
```

### 41.3 Setup w EventMaster

**1. Dodaj dependencje (pom.xml):**
```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
```

**2. Bazowa klasa testowa:**
```java
@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${kafka.bootstrap-servers}",
    "spring.datasource.url=${postgres.url}",
    "spring.datasource.username=${postgres.username}",
    "spring.datasource.password=${postgres.password}"
})
abstract class IntegrationTestBase {
    
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
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
```

### 41.4 Test Pełnego Flow w EventMaster

**Scenariusz:** Użytkownik tworzy event → zapisuje do PostgreSQL → publikuje na Kafka → konsument aktualizuje read model

```java
@SpringBootTest
class CreateEventE2ETest extends IntegrationTestBase {
    
    @Autowired
    private EventCommandService commandService;
    
    @Autowired
    private EventQueryService queryService;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Test
    void shouldCreateEventEndToEnd() throws Exception {
        // Given
        var command = new CreateEventCommand(
            "Test Event",
            "Description",
            LocalDateTime.now().plusDays(7),
            "test-organizer-id"
        );
        
        // When: Wysyłamy command
        var eventId = commandService.createEvent(command);
        
        // Then: Event zapisany w PostgreSQL (write model)
        var writeModel = jdbcTemplate.queryForMap(
            "SELECT * FROM events WHERE id = ?", eventId
        );
        assertThat(writeModel)
            .containsEntry("title", "Test Event")
            .containsEntry("status", "DRAFT");
        
        // And: Event opublikowany na Kafka
        await().atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> {
                var messages = kafkaConsumer.poll(Duration.ofSeconds(1));
                assertThat(messages).isNotEmpty();
            });
        
        // And: Read model zaktualizowany przez projektora
        await().atMost(Duration.ofSeconds(15))
            .untilAsserted(() -> {
                var readModel = queryService.findById(eventId);
                assertThat(readModel).isPresent();
                assertThat(readModel.get().getTitle()).isEqualTo("Test Event");
            });
    }
}
```

### 41.5 Testowanie Eventual Consistency

```java
@Test
void shouldHandleEventualConsistency() {
    // Given: Event created
    var eventId = commandService.createEvent(command);
    
    // When: Natychmiast po utworzeniu
    var immediately = queryService.findById(eventId);
    
    // Then: Read model może jeszcze nie być gotowy (eventual consistency!)
    assertThat(immediately).isEmpty();  // To jest OK!
    
    // But: Po chwili powinien być dostępny
    await().atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> {
            var readModel = queryService.findById(eventId);
            assertThat(readModel).isPresent();
        });
}
```

### 41.6 Test Transactional Outbox

```java
@Test
void shouldUseTransactionalOutbox() {
    // Given
    var command = new CreateEventCommand(...);
    
    // When: Simulujemy failure Kafka
    kafka.stop();  // Kafka niedostępna!
    
    var eventId = commandService.createEvent(command);
    
    // Then: Event saved w bazie
    assertEventExists(eventId);
    
    // And: Message w outbox table
    var outboxMessages = jdbcTemplate.queryForList(
        "SELECT * FROM outbox WHERE aggregate_id = ?", eventId
    );
    assertThat(outboxMessages).hasSize(1);
    assertThat(outboxMessages.get(0))
        .containsEntry("event_type", "EventCreatedEvent")
        .containsEntry("published", false);
    
    // When: Kafka wraca
    kafka.start();
    
    // Then: Outbox publisher wyśle message
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> {
            var published = jdbcTemplate.queryForObject(
                "SELECT published FROM outbox WHERE aggregate_id = ?",
                Boolean.class, eventId
            );
            assertThat(published).isTrue();
        });
}
```

### 41.7 Test Idempotency

```java
@Test
void shouldBeIdempotent() {
    // Given: Command z idempotency key
    var command = new CreateEventCommand(...);
    command.setIdempotencyKey("unique-key-123");
    
    // When: Wysyłamy 3 razy (np. retry)
    var eventId1 = commandService.createEvent(command);
    var eventId2 = commandService.createEvent(command);
    var eventId3 = commandService.createEvent(command);
    
    // Then: Tylko jeden event utworzony
    assertThat(eventId1).isEqualTo(eventId2).isEqualTo(eventId3);
    
    var events = jdbcTemplate.queryForList(
        "SELECT * FROM events WHERE idempotency_key = ?",
        "unique-key-123"
    );
    assertThat(events).hasSize(1);
}
```

### 41.8 Wzorce Testowe dla Testcontainers

**Pattern 1: Singleton Containers (szybsze testy)**
```java
@Testcontainers
class FastIntegrationTest {
    
    // Singleton - container startuje raz dla wszystkich testów
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18")
        .withReuse(true);  // Reuse między uruchomieniami!
}
```

**Pattern 2: Per-Test Cleanup**
```java
@BeforeEach
void cleanupDatabase() {
    jdbcTemplate.execute("TRUNCATE TABLE events CASCADE");
    jdbcTemplate.execute("DELETE FROM outbox");
}
```

**Pattern 3: Test Data Builders**
```java
class EventTestDataBuilder {
    private String title = "Default Title";
    private LocalDateTime startDate = LocalDateTime.now().plusDays(7);
    
    public EventTestDataBuilder withTitle(String title) {
        this.title = title;
        return this;
    }
    
    public CreateEventCommand build() {
        return new CreateEventCommand(title, "Description", startDate, "organizer-1");
    }
}

// Usage:
@Test
void test() {
    var command = new EventTestDataBuilder()
        .withTitle("My Event")
        .build();
}
```

### 41.9 Debugging Testcontainers

```java
@Test
void debugTestcontainers() {
    System.out.println("PostgreSQL: " + postgres.getJdbcUrl());
    System.out.println("Kafka: " + kafka.getBootstrapServers());
    
    // Access logs
    String logs = postgres.getLogs();
    System.out.println("PostgreSQL logs:\n" + logs);
}
```

### 41.10 Testy Wydajnościowe z Testcontainers

```java
@Test
void shouldHandleHighLoad() {
    // Given: 100 concurrent users
    var executor = Executors.newFixedThreadPool(100);
    var latch = new CountDownLatch(100);
    
    var startTime = System.currentTimeMillis();
    
    // When: 100 równoczesnych CreateEventCommand
    for (int i = 0; i < 100; i++) {
        executor.submit(() -> {
            try {
                commandService.createEvent(new EventTestDataBuilder().build());
            } finally {
                latch.countDown();
            }
        });
    }
    
    latch.await(30, TimeUnit.SECONDS);
    var duration = System.currentTimeMillis() - startTime;
    
    // Then: Wszystkie eventy utworzone w < 10s
    assertThat(duration).isLessThan(10_000);
    
    var count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM events", Long.class);
    assertThat(count).isEqualTo(100);
}
```

### 41.11 Podsumowanie

**Testcontainers dają Ci:**
- ✅ Pewność, że kod działa z prawdziwymi systemami
- ✅ Wykrywanie problemów, których mocki nie catch'ują
- ✅ CI/CD ready (Docker w Docker)
- ✅ Izolację między testami

**Best Practices:**
- Używaj singleton containers dla szybkości
- Cleanuj dane między testami
- Testuj eventual consistency z `await()`
- Używaj test data builders

**Następny Temat:** [Contract Testing z Pact](#temat-42-contract-testing-pact)

---
## Temat 42: Contract Testing with Pact

### 42.1 Definicja

**Contract Testing** weryfikuje, czy API producer (backend) i consumer (frontend) zgadzają się co do **kontraktu** - formatu requestów i responses.

**Analogia:**
- Ty (consumer) oczekujesz, że zamówisz "kawę latte"
- Barista (producer) rozumie "coffee latte"
- **Kontrakt niespełniony!** ☕❌

### 42.2 Problem bez Contract Testing

```typescript
// Frontend (Consumer)
const event = await fetch('/api/events/123');
console.log(event.organizer_id);  // oczekuje organizer_id

// Backend (Producer) - ktoś zmienił pole!
{
  "organizerId": "123"  // camelCase zamiast snake_case!
}
// Frontend sie wysypuje! 💥
```

### 42.3 Pact w EventMaster

**1. Setup (pom.xml):**
```xml
<dependency>
    <groupId>au.com.dius.pact.provider</groupId>
    <artifactId>junit5</artifactId>
    <version>4.6.3</version>
    <scope>test</scope>
</dependency>
```

**2. Consumer Test (Frontend/Nuxt):**
```typescript
// tests/pact/event.contract.spec.ts
import { PactV3 } from '@pact-foundation/pact';

describe('Event API Contract', () => {
  const provider = new PactV3({
    consumer: 'EventMasterFrontend',
    provider: 'EventMasterBackend',
  });

  it('should get event by ID', async () => {
    await provider
      .given('event 123 exists')
      .uponReceiving('a request for event 123')
      .withRequest({
        method: 'GET',
        path: '/api/events/123',
        headers: {
          'Authorization': 'Bearer token123'
        }
      })
      .willRespondWith({
        status: 200,
        headers: {
          'Content-Type': 'application/json'
        },
        body: {
          id: '123',
          title: 'Test Event',
          organizer_id: '456',  // KONTRAKT: snake_case!
          start_date: '2025-02-01T10:00:00Z'
        }
      })
      .executeTest(async (mockServer) => {
        // Test z mockserwera
        const response = await fetch(`${mockServer.url}/api/events/123`);
        const event = await response.json();
        
        expect(event.organizer_id).toBe('456');
      });
  });
});
```

**3. Producer Test (Backend/Spring Boot):**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Provider("EventMasterBackend")
@PactBroker(host = "localhost", port = "9292")
class EventApiContractTest {
    
    @LocalServerPort
    private int port;
    
    @Autowired
    private EventRepository repository;
    
    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void pactVerificationTestTemplate(PactVerificationContext context) {
        context.verifyInteraction();
    }
    
    @BeforeEach
    void setupTestTarget(PactVerificationContext context) {
        context.setTarget(new HttpTestTarget("localhost", port));
    }
    
    @State("event 123 exists")
    void eventExists() {
        // Setup test data zgodnie z kontraktem
        var event = new Event();
        event.setId("123");
        event.setTitle("Test Event");
        event.setOrganizerId("456");  // snake_case mapping!
        repository.save(event);
    }
}
```

### 42.4 Pact Broker

**Deployment Pact Broker (docker-compose.yml):**
```yaml
  pact-broker:
    image: pactfoundation/pact-broker:latest
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "9292:9292"
    environment:
      PACT_BROKER_DATABASE_URL: postgresql://user:password@postgres/pact_broker
      PACT_BROKER_DATABASE_NAME: pact_broker
      PACT_BROKER_LOG_LEVEL: INFO
```

### 42.5 CI/CD Integration

```yaml
# .github/workflows/contract-tests.yml
name: Contract Tests

on: [push, pull_request]

jobs:
  consumer:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run Consumer Tests
        run: npm test -- tests/pact/
      
      - name: Publish Pacts to Broker
        run: npm run pact:publish
        env:
          PACT_BROKER_BASE_URL: ${{ secrets.PACT_BROKER_URL }}

  provider:
    needs: consumer
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run Provider Verification
        run: mvn test -Dtest=EventApiContractTest
        env:
          PACT_BROKER_BASE_URL: ${{ secrets.PACT_BROKER_URL }}
```

### 42.6 Can I Deploy?

Pact ma **can-i-deploy** tool sprawdzający, czy możesz wdrożyć!

```bash
# Czy mogę wdrożyć Frontend v1.2.3?
npx pact-broker can-i-deploy \
  --pacticipant EventMasterFrontend \
  --version 1.2.3 \
  --to production

# Output:
# ✅ Computer says yes! All verifications passed.
```

### 42.7 Podsumowanie

**Contract Testing:**
- ✅ Catch breaking changes PRZED produkcją
- ✅ Dokumentacja API "z testów"
- ✅ Szybsze niż E2E testy
- ✅ Niezależny development frontend/backend

---

## Temat 43: Chaos Engineering

### 43.1 Definicja

**Chaos Engineering** to praktyka celowego wprowadzania **awarii** w systemie, aby sprawdzić jego odporność.

**Filozofia:** Jeśli nie testujesz failure, failure zatestujesz Ciebie na produkcji. 🔥

### 43.2 Chaos w EventMaster

**Narzędzie:** Chaos Monkey (Netflix) / Toxiproxy

**1. Setup Toxiproxy (docker-compose.yml):**
```yaml
  toxiproxy:
    image: ghcr.io/shopify/toxiproxy:latest
    ports:
      - "8474:8474"  # API
      - "5433:5433"  # PostgreSQL proxy
      - "9093:9093"  # Kafka proxy
```

**2. Konfiguracja Proxies:**
```bash
# Proxy dla PostgreSQL
toxiproxy-cli create postgres_proxy \
  --listen 0.0.0.0:5433 \
  --upstream postgres:5432

# Proxy dla Kafka
toxiproxy-cli create kafka_proxy \
  --listen 0.0.0.0:9093 \
  --upstream kafka:9092
```

### 43.3 Scenariusze Chaos

**Scenario 1: Database Latency**
```java
@Test
void shouldHandleDatabaseLatency() {
    // Given: Dodaj 1000ms latency do PostgreSQL
    toxiproxy.addToxic("latency", "latency", "downstream", 1.0, 
        Map.of("latency", 1000));
    
    // When: Tworzenie eventa
    var start = System.currentTimeMillis();
    var eventId = commandService.createEvent(command);
    var duration = System.currentTimeMillis() - start;
    
    // Then: Timeout nie powinien wystąpić (circuit breaker!)
    assertThat(duration).isLessThan(5000);
    assertThat(eventId).isNotNull();
}
```

**Scenario 2: Network Partition**
```java
@Test
void shouldHandleKafkaPartition() {
    // Given: Kafka niedostępna
    toxiproxy.disable("kafka_proxy");
    
    // When: Próba publikacji eventu
    var eventId = commandService.createEvent(command);
    
    // Then: Używamy Transactional Outbox!
    assertThat(eventId).isNotNull();
    
    var outboxCount = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM outbox WHERE published = false",
        Long.class
    );
    assertThat(outboxCount).isEqualTo(1);
    
    // When: Kafka wraca
    toxiproxy.enable("kafka_proxy");
    
    // Then: Outbox publisher wyśle message
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> {
            var published = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE published = true",
                Long.class
            );
            assertThat(published).isEqualTo(1);
        });
}
```

**Scenario 3: Bandwidth Limit**
```java
@Test
void shouldHandleLimitedBandwidth() {
    // Given: Ogranicz bandwidth do 10KB/s
    toxiproxy.addToxic("bandwidth", "bandwidth", "downstream", 1.0,
        Map.of("rate", 10));  // 10 KB/s
    
    // When: Duże zapytanie (np. lista 1000 eventów)
    var events = queryService.findAll(PageRequest.of(0, 1000));
    
    // Then: Działa, ale wolno
    assertThat(events).isNotEmpty();
}
```

**Scenario 4: Random Connection Close**
```java
@Test
void shouldHandleConnectionLoss() {
    // Given: 50% requestów się nie udaje
    toxiproxy.addToxic("timeout", "timeout", "downstream", 0.5,
        Map.of("timeout", 0));
    
    // When: 100 requestów
    var successes = 0;
    for (int i = 0; i < 100; i++) {
        try {
            commandService.createEvent(command);
            successes++;
        } catch (Exception e) {
            // OK, spodziewamy się failures
        }
    }
    
    // Then: Przynajmniej część się udała (retry mechanizm!)
    assertThat(successes).isGreaterThan(0);
}
```

### 43.4 Circuit Breaker w EventMaster

**1. Dodaj Resilience4j:**
```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.1.0</version>
</dependency>
```

**2. Konfiguracja:**
```yaml
resilience4j:
  circuitbreaker:
    instances:
      eventService:
        failure-rate-threshold: 50
        wait-duration-in-open-state: 10s
        permitted-number-of-calls-in-half-open-state: 3
        sliding-window-size: 10
        minimum-number-of-calls: 5
```

**3. Implementacja:**
```java
@Service
public class EventCommandService {
    
    @CircuitBreaker(name = "eventService", fallbackMethod = "createEventFallback")
    public UUID createEvent(CreateEventCommand command) {
        // Normal flow
        var event = eventFactory.create(command);
        repository.save(event);
        kafkaTemplate.send("events", event);
        return event.getId();
    }
    
    // Fallback gdy circuit breaker open
    public UUID createEventFallback(CreateEventCommand command, Exception e) {
        log.warn("Circuit breaker activated, using fallback", e);
        
        // Zapisz tylko do bazy, Kafka później
        var event = eventFactory.create(command);
        repository.save(event);
        
        // Outbox zadziała później
        outboxService.schedule(event);
        
        return event.getId();
    }
}
```

**4. Test Circuit Breaker:**
```java
@Test
void shouldOpenCircuitBreaker() {
    // Given: Kafka down
    toxiproxy.disable("kafka_proxy");
    
    // When: 10 requestów (próg = 5, failure rate = 50%)
    for (int i = 0; i < 10; i++) {
        commandService.createEvent(command);
    }
    
    // Then: Circuit Breaker powinien być OPEN
    var metrics = circuitBreakerRegistry.circuitBreaker("eventService").getMetrics();
    assertThat(metrics.getFailureRate()).isGreaterThan(50);
    
    // And: Fallback method użyty
    verify(outboxService, atLeastOnce()).schedule(any());
}
```

### 43.5 Retry Mechanisms

```java
@Service
public class EventQueryService {
    
    @Retry(name = "database", fallbackMethod = "findByIdFromCache")
    public Optional<EventView> findById(UUID id) {
        return repository.findById(id);
    }
    
    public Optional<EventView> findByIdFromCache(UUID id, Exception e) {
        log.warn("Database unavailable, using cache", e);
        return cacheService.get(id);
    }
}
```

**Konfiguracja retry:**
```yaml
resilience4j:
  retry:
    instances:
      database:
        max-attempts: 3
        wait-duration: 1s
        retry-exceptions:
          - java.sql.SQLException
          - org.springframework.dao.DataAccessException
```

### 43.6 Podsumowanie Chaos Engineering

**Testowane scenariusze:**
- ✅ Database latency
- ✅ Network partition
- ✅ Bandwidth limitations
- ✅ Random connection losses
- ✅ Service unavailability

**Obrona:**
- ✅ Circuit Breakers
- ✅ Retry with backoff
- ✅ Fallback methods
- ✅ Transactional Outbox
- ✅ Graceful degradation

**Narzędzia:**
- Toxiproxy
- Chaos Monkey
- Resilience4j

---
## Temat 44: Performance Testing

### 44.1 Rodzaje Testów Wydajnościowych

1. **Load Testing** - Jak system radzi sobie pod normalnym obciążeniem?
2. **Stress Testing** - Gdzie jest punkt załamania?
3. **Spike Testing** - Jak system reaguje na nagły wzrost ruchu?
4. **Soak Testing** - Czy system wytrzyma 24h pod obciążeniem?

### 44.2 Narzędzia

**Gatling** - Scala-based load testing tool (preferowane dla JVM)

**Setup (pom.xml):**
```xml
<dependency>
    <groupId>io.gatling.highcharts</groupId>
    <artifactId>gatling-charts-highcharts</artifactId>
    <version>3.10.3</version>
    <scope>test</scope>
</dependency>
```

### 44.3 Load Test - CreateEvent

```scala
// src/test/scala/simulations/CreateEventSimulation.scala
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

class CreateEventSimulation extends Simulation {
  
  val httpProtocol = http
    .baseUrl("http://localhost:8080")
    .acceptHeader("application/json")
    .authorizationHeader("Bearer ${access_token}")
  
  val createEventScenario = scenario("Create Event")
    .exec(http("Login")
      .post("/api/auth/login")
      .body(StringBody("""{"username":"test","password":"test"}"""))
      .check(jsonPath("$.access_token").saveAs("access_token")))
    .pause(1.second)
    .exec(http("Create Event")
      .post("/api/events")
      .body(StringBody("""
        {
          "title": "Load Test Event ${__UUID()}",
          "description": "Performance test",
          "start_date": "2025-03-01T10:00:00Z",
          "end_date": "2025-03-01T18:00:00Z",
          "location": "Test Location"
        }
      """))
      .check(status.is(201))
      .check(jsonPath("$.id").saveAs("eventId")))
    .pause(1.second)
    .exec(http("Get Event")
      .get("/api/events/${eventId}")
      .check(status.is(200)))
  
  setUp(
    createEventScenario.inject(
      // Ramping: 0 → 100 users w 5 minut
      rampUsers(100).during(5.minutes),
      // Constant: 100 users przez 10 minut
      constantUsersPerSec(20).during(10.minutes),
      // Spike: 500 users nagle!
      atOnceUsers(500)
    )
  ).protocols(httpProtocol)
   .assertions(
     global.responseTime.max.lt(2000),  // Max 2s
     global.successfulRequests.percent.gt(95)  // 95% success rate
   )
}
```

### 44.4 Uruchomienie Load Test

```bash
# Run Gatling simulation
mvn gatling:test -Dgatling.simulationClass=simulations.CreateEventSimulation

# Output:
================================================================================
---- Global Information --------------------------------------------------------
> request count                                      15000 (OK=14850   KO=150  )
> min response time                                     12 (OK=12      KO=1205 )
> max response time                                   1987 (OK=1987    KO=5001 )
> mean response time                                   245 (OK=232     KO=2456 )
> std deviation                                        187 (OK=145     KO=1234 )
> response time 50th percentile                        189 (OK=185     KO=2301 )
> response time 75th percentile                        324 (OK=312     KO=3105 )
> response time 95th percentile                        612 (OK=598     KO=4567 )
> response time 99th percentile                        987 (OK=945     KO=4901 )
> mean requests/sec                                 25.000 (OK=24.750  KO=0.250)
---- Response Time Distribution ------------------------------------------------
> t < 800 ms                                         14234 ( 95%)
> 800 ms < t < 1200 ms                                 456 (  3%)
> t > 1200 ms                                          160 (  1%)
> failed                                               150 (  1%)
================================================================================
```

### 44.5 Database Performance

**Slow Query Detection:**
```sql
-- PostgreSQL 18: Enable slow query log
ALTER SYSTEM SET log_min_duration_statement = 100;  -- Log queries > 100ms
SELECT pg_reload_conf();

-- View slow queries
SELECT 
  query,
  calls,
  total_exec_time / 1000 as total_time_seconds,
  mean_exec_time / 1000 as avg_time_seconds,
  max_exec_time / 1000 as max_time_seconds
FROM pg_stat_statements
WHERE mean_exec_time > 100  -- > 100ms average
ORDER BY mean_exec_time DESC
LIMIT 10;
```

**Indexes dla EventMaster:**
```sql
-- Przed load testem sprawdź indexes!
CREATE INDEX CONCURRENTLY idx_events_organizer_id ON events(organizer_id);
CREATE INDEX CONCURRENTLY idx_events_start_date ON events(start_date);
CREATE INDEX CONCURRENTLY idx_events_status ON events(status);

-- Composite index dla częstych zapytań
CREATE INDEX CONCURRENTLY idx_events_organizer_status 
  ON events(organizer_id, status) 
  INCLUDE (title, start_date);

-- Full-text search index
CREATE INDEX CONCURRENTLY idx_events_fulltext 
  ON events USING gin(to_tsvector('english', title || ' ' || description));
```

### 44.6 Kafka Performance

**Throughput Test:**
```bash
# Producer throughput
kafka-producer-perf-test \
  --topic events \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# Output:
999998 records sent, 166388.869258 records/sec (162.49 MB/sec), 
avg latency 345.32 ms, max latency 2567.00 ms

# Consumer throughput
kafka-consumer-perf-test \
  --bootstrap-server localhost:9092 \
  --topic events \
  --messages 1000000

# Output:
MB.sec: 158.3421
nMsg.sec: 162134.1234
```

**Optimize Producer:**
```java
@Bean
public ProducerFactory<String, Event> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    
    // Performance tuning
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB batches
    config.put(ProducerConfig.LINGER_MS_CONFIG, 10);  // Wait 10ms for batch
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // Compress
    config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64MB buffer
    config.put(ProducerConfig.ACKS_CONFIG, "1");  // Leader ack only (fast!)
    
    return new DefaultKafkaProducerFactory<>(config);
}
```

### 44.7 JVM Performance Tuning

**application.properties:**
```properties
# Spring Boot optimizations
spring.jpa.hibernate.ddl-auto=validate
spring.jpa.show-sql=false
spring.jpa.properties.hibernate.jdbc.batch_size=50
spring.jpa.properties.hibernate.order_inserts=true
spring.jpa.properties.hibernate.order_updates=true

# Connection pooling (HikariCP)
spring.datasource.hikari.maximum-pool-size=20
spring.datasource.hikari.minimum-idle=5
spring.datasource.hikari.connection-timeout=30000
spring.datasource.hikari.idle-timeout=600000
spring.datasource.hikari.max-lifetime=1800000
```

**JVM flags:**
```bash
# GC tuning dla low-latency
java -Xms2g -Xmx2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+ParallelRefProcEnabled \
  -XX:+UseStringDeduplication \
  -jar eventmaster.jar
```

### 44.8 Monitoring during Load Test

```java
@Configuration
public class MetricsConfig {
    
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return registry -> registry.config()
            .commonTags("application", "eventmaster");
    }
}

@Service
public class EventCommandService {
    
    private final Counter eventCreatedCounter;
    private final Timer eventCreationTimer;
    
    public EventCommandService(MeterRegistry registry) {
        this.eventCreatedCounter = registry.counter("events.created");
        this.eventCreationTimer = registry.timer("events.creation.time");
    }
    
    public UUID createEvent(CreateEventCommand command) {
        return eventCreationTimer.record(() -> {
            var event = createEventInternal(command);
            eventCreatedCounter.increment();
            return event.getId();
        });
    }
}
```

**Expose metrics:**
```properties
management.endpoints.web.exposure.include=health,info,metrics,prometheus
management.metrics.export.prometheus.enabled=true
```

**Grafana Dashboard:**
```yaml
# docker-compose.yml
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
```

### 44.9 Performance Test Checklist

**Przed testem:**
- [ ] Indexes na miejscu?
- [ ] Connection pool skonfigurowany?
- [ ] Kafka partitions wystarczające? (min. = liczba consumerów)
- [ ] JVM heap size odpowiedni?
- [ ] Monitoring włączony?

**Podczas testu:**
- [ ] CPU < 80%?
- [ ] Memory nie rośnie liniowo? (memory leak!)
- [ ] Database connections nie exhausted?
- [ ] Kafka lag < 1000 messages?
- [ ] Response time < SLA?

**Po teście:**
- [ ] Analyze slow queries
- [ ] Check GC logs
- [ ] Review error logs
- [ ] Compare with baseline

### 44.10 Podsumowanie

**Performance Testing w EventMaster:**
- ✅ Gatling dla load testing
- ✅ PostgreSQL query optimization
- ✅ Kafka throughput testing
- ✅ JVM tuning
- ✅ Metrics & monitoring

**Next:** [Monitoring & Observability](#temat-45-monitoring-observability)

---

## Temat 45: Monitoring & Observability

### 45.1 3 Pillars of Observability

1. **Metrics** - Liczby (CPU, memory, request rate)
2. **Logs** - Wydarzenia (errors, warnings, info)
3. **Traces** - Request flow przez system

### 45.2 Metrics z Micrometer + Prometheus

**1. Dependencies:**
```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

**2. Expose Prometheus endpoint:**
```properties
management.endpoints.web.exposure.include=prometheus,health,info,metrics
management.metrics.export.prometheus.enabled=true
management.endpoint.prometheus.enabled=true
```

**3. Custom Metrics:**
```java
@Component
public class EventMetrics {
    
    private final Counter eventsCreated;
    private final Counter eventsPublished;
    private final Counter eventsFailed;
    private final Gauge activeEvents;
    private final Timer eventCreationTime;
    
    public EventMetrics(MeterRegistry registry, EventRepository repository) {
        this.eventsCreated = Counter.builder("eventmaster.events.created")
            .description("Total events created")
            .tag("type", "command")
            .register(registry);
        
        this.eventsPublished = Counter.builder("eventmaster.events.published")
            .description("Events published to Kafka")
            .register(registry);
        
        this.eventsFailed = Counter.builder("eventmaster.events.failed")
            .description("Failed event operations")
            .tag("reason", "unknown")
            .register(registry);
        
        this.activeEvents = Gauge.builder("eventmaster.events.active", repository,
                repo -> repo.countByStatus(EventStatus.ACTIVE))
            .description("Currently active events")
            .register(registry);
        
        this.eventCreationTime = Timer.builder("eventmaster.events.creation.time")
            .description("Time to create event")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    }
    
    public void recordEventCreated() {
        eventsCreated.increment();
    }
    
    public void recordEventPublished() {
        eventsPublished.increment();
    }
    
    public void recordEventFailed(String reason) {
        Counter.builder("eventmaster.events.failed")
            .tag("reason", reason)
            .register(meterRegistry)
            .increment();
    }
    
    public <T> T recordCreationTime(Supplier<T> operation) {
        return eventCreationTime.record(operation);
    }
}
```

**4. Usage:**
```java
@Service
public class EventCommandService {
    
    @Autowired
    private EventMetrics metrics;
    
    public UUID createEvent(CreateEventCommand command) {
        return metrics.recordCreationTime(() -> {
            try {
                var event = eventFactory.create(command);
                repository.save(event);
                kafkaTemplate.send("events", event);
                
                metrics.recordEventCreated();
                metrics.recordEventPublished();
                
                return event.getId();
            } catch (Exception e) {
                metrics.recordEventFailed(e.getClass().getSimpleName());
                throw e;
            }
        });
    }
}
```

**5. Prometheus scraping (prometheus.yml):**
```yaml
scrape_configs:
  - job_name: 'eventmaster-backend'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['backend:8080']
  
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']
  
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-exporter:9308']
```

### 45.3 Structured Logging z Logback

**logback-spring.xml:**
```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeMdcKeyName>trace_id</includeMdcKeyName>
            <includeMdcKeyName>span_id</includeMdcKeyName>
            <includeMdcKeyName>user_id</includeMdcKeyName>
            <includeMdcKeyName>event_id</includeMdcKeyName>
        </encoder>
    </appender>
    
    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/eventmaster.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/eventmaster.%d{yyyy-MM-dd}.log.gz</fileNamePattern>
            <maxHistory>30</maxHistory>
        </rollingPolicy>
        <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="FILE"/>
    </root>
</configuration>
```

**Usage:**
```java
@Slf4j
@Service
public class EventCommandService {
    
    public UUID createEvent(CreateEventCommand command) {
        MDC.put("user_id", command.getOrganizerId());
        
        log.info("Creating event: title={}, start_date={}", 
            command.getTitle(), command.getStartDate());
        
        try {
            var eventId = createEventInternal(command);
            MDC.put("event_id", eventId.toString());
            
            log.info("Event created successfully");
            return eventId;
        } catch (Exception e) {
            log.error("Failed to create event", e);
            throw e;
        } finally {
            MDC.clear();
        }
    }
}
```

**JSON output:**
```json
{
  "timestamp": "2025-01-20T15:30:45.123Z",
  "level": "INFO",
  "logger": "com.eventmaster.EventCommandService",
  "message": "Event created successfully",
  "trace_id": "abc123",
  "span_id": "def456",
  "user_id": "user-789",
  "event_id": "evt-999"
}
```

### 45.4 Centralized Logging (ELK Stack)

**docker-compose.yml:**
```yaml
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    ports:
      - "9200:9200"
    volumes:
      - es-data:/usr/share/elasticsearch/data
  
  logstash:
    image: docker.elastic.co/logstash/logstash:8.11.0
    ports:
      - "5000:5000"
    volumes:
      - ./logstash.conf:/usr/share/logstash/pipeline/logstash.conf
    depends_on:
      - elasticsearch
  
  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    ports:
      - "5601:5601"
    environment:
      ELASTICSEARCH_HOSTS: http://elasticsearch:9200
    depends_on:
      - elasticsearch
```

**logstash.conf:**
```ruby
input {
  tcp {
    port => 5000
    codec => json
  }
}

filter {
  if [level] == "ERROR" {
    mutate {
      add_tag => ["error"]
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "eventmaster-%{+YYYY.MM.dd}"
  }
}
```

### 45.5 Alerting z Prometheus Alertmanager

**alert.rules.yml:**
```yaml
groups:
  - name: eventmaster
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: rate(eventmaster_events_failed_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors/sec"
      
      - alert: SlowResponseTime
        expr: histogram_quantile(0.95, eventmaster_events_creation_time_bucket) > 2
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Slow response time"
          description: "P95 latency is {{ $value }}s"
      
      - alert: KafkaLag
        expr: kafka_consumer_lag > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer lag high"
          description: "Lag is {{ $value }} messages"
```

### 45.6 Health Checks

```java
@Component
public class KafkaHealthIndicator implements HealthIndicator {
    
    @Autowired
    private KafkaAdmin kafkaAdmin;
    
    @Override
    public Health health() {
        try {
            var adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
            var result = adminClient.describeCluster();
            result.nodes().get(10, TimeUnit.SECONDS);
            
            return Health.up()
                .withDetail("cluster_id", result.clusterId().get())
                .build();
        } catch (Exception e) {
            return Health.down()
                .withException(e)
                .build();
        }
    }
}

@Component
public class DatabaseHealthIndicator implements HealthIndicator {
    
    @Autowired
    private DataSource dataSource;
    
    @Override
    public Health health() {
        try (Connection conn = dataSource.getConnection()) {
            var valid = conn.isValid(2);
            return valid ? Health.up().build() : Health.down().build();
        } catch (SQLException e) {
            return Health.down().withException(e).build();
        }
    }
}
```

**Aggregated health endpoint:**
```bash
curl http://localhost:8080/actuator/health

{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP"
    },
    "kafka": {
      "status": "UP",
      "details": {
        "cluster_id": "kafka-cluster-1"
      }
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 500GB,
        "free": 250GB
      }
    }
  }
}
```

### 45.7 Podsumowanie

**Observability Stack dla EventMaster:**
- ✅ Metrics: Micrometer + Prometheus + Grafana
- ✅ Logs: Logback + ELK Stack
- ✅ Traces: OpenTelemetry (następny temat!)
- ✅ Alerting: Prometheus Alertmanager
- ✅ Health Checks: Spring Boot Actuator

---
## Temat 46: Distributed Tracing

### 46.1 Problem w Systemach Rozproszonych

Request przechodzi przez wiele serwisów:
```
Frontend → Backend → Kafka → Consumer → Database
```

Jak debug'ować gdy coś nie działa? **Distributed Tracing!**

### 46.2 OpenTelemetry Setup

**1. Dependencies:**
```xml
<dependency>
    <groupId>io.opentelemetry.instrumentation</groupId>
    <artifactId>opentelemetry-spring-boot-starter</artifactId>
    <version>2.0.0</version>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
</dependency>
```

**2. Configuration:**
```properties
otel.service.name=eventmaster-backend
otel.traces.exporter=otlp
otel.exporter.otlp.endpoint=http://jaeger:4317
otel.metrics.exporter=none
```

**3. Jaeger (docker-compose.yml):**
```yaml
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # UI
      - "4317:4317"    # OTLP gRPC
    environment:
      COLLECTOR_OTLP_ENABLED: true
```

### 46.3 Automatic Instrumentation

Spring Boot + OpenTelemetry automatycznie instrument:
- HTTP requests
- Database queries
- Kafka produce/consume
- Redis operations

**Przykład trace:**
```
Trace ID: abc123xyz789

Span 1: HTTP POST /api/events (150ms)
  └─ Span 2: EventCommandService.createEvent (145ms)
      ├─ Span 3: EventRepository.save (50ms)
      │   └─ Span 4: SQL INSERT (45ms)
      └─ Span 5: KafkaTemplate.send (90ms)
          └─ Span 6: Kafka produce (85ms)

Span 7: Kafka consume events (5ms)
  └─ Span 8: EventProjector.project (120ms)
      └─ Span 9: EventViewRepository.save (115ms)
          └─ Span 10: SQL INSERT (110ms)
```

### 46.4 Custom Spans

```java
@Service
public class EventCommandService {
    
    @Autowired
    private Tracer tracer;
    
    public UUID createEvent(CreateEventCommand command) {
        Span span = tracer.spanBuilder("createEvent")
            .setAttribute("event.title", command.getTitle())
            .setAttribute("organizer.id", command.getOrganizerId())
            .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Business logic
            var eventId = createEventInternal(command);
            
            span.setAttribute("event.id", eventId.toString());
            span.setStatus(StatusCode.OK);
            
            return eventId;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
```

### 46.5 Trace Context Propagation

**Kafka Headers:**
```java
@Service
public class EventPublisher {
    
    public void publishEvent(Event event) {
        var headers = new RecordHeaders();
        
        // Inject trace context
        W3CTraceContextPropagator.getInstance().inject(
            Context.current(),
            headers,
            (carrier, key, value) -> carrier.add(key, value.getBytes())
        );
        
        kafkaTemplate.send(new ProducerRecord<>(
            "events",
            null,
            event.getId().toString(),
            event,
            headers
        ));
    }
}

@KafkaListener(topics = "events")
public void consumeEvent(ConsumerRecord<String, Event> record) {
    // Extract trace context
    var context = W3CTraceContextPropagator.getInstance().extract(
        Context.current(),
        record.headers(),
        (carrier, key) -> {
            Header header = carrier.lastHeader(key);
            return header != null ? new String(header.value()) : null;
        }
    );
    
    // Continue trace
    try (Scope scope = context.makeCurrent()) {
        projectEvent(record.value());
    }
}
```

### 46.6 Jaeger UI

Otwórz http://localhost:16686

**Filtry:**
- Service: eventmaster-backend
- Operation: POST /api/events
- Min Duration: > 100ms
- Tags: error=true

**Analiza:**
- Który span jest najwolniejszy?
- Gdzie są błędy?
- Czy są niepotrzebne N+1 queries?

---

## Temat 47: Health Checks & Readiness

### 47.1 Liveness vs Readiness

**Liveness:** Czy aplikacja żyje? (Kubernetes restart jeśli nie)
**Readiness:** Czy aplikacja gotowa przyjmować ruch? (Kubernetes nie routuje jeśli nie)

### 47.2 Spring Boot Actuator

```properties
management.endpoint.health.probes.enabled=true
management.health.livenessState.enabled=true
management.health.readinessState.enabled=true
```

**Endpoints:**
- `/actuator/health/liveness` - czy żyje?
- `/actuator/health/readiness` - czy gotowa?

### 47.3 Custom Readiness Checks

```java
@Component
public class EventMasterReadinessCheck implements HealthIndicator {
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private KafkaAdmin kafkaAdmin;
    
    @Override
    public Health health() {
        // Check database
        if (!isDatabaseReady()) {
            return Health.down()
                .withDetail("reason", "Database not ready")
                .build();
        }
        
        // Check Kafka
        if (!isKafkaReady()) {
            return Health.down()
                .withDetail("reason", "Kafka not ready")
                .build();
        }
        
        // Check migrations applied
        if (!areMigrationsApplied()) {
            return Health.down()
                .withDetail("reason", "Migrations pending")
                .build();
        }
        
        return Health.up().build();
    }
}
```

### 47.4 Kubernetes Integration

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventmaster-backend
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: backend
        image: eventmaster-backend:1.0.0
        ports:
        - containerPort: 8080
        
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 20
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
        
        startupProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 0
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 30  # 30 * 5s = 150s max startup time
```

### 47.5 Graceful Shutdown

```properties
server.shutdown=graceful
spring.lifecycle.timeout-per-shutdown-phase=30s
```

```java
@Component
public class GracefulShutdownHandler {
    
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    
    @PreDestroy
    public void onShutdown() {
        log.info("Graceful shutdown initiated");
        
        // Stop consuming new messages
        registry.getListenerContainers().forEach(container -> {
            container.stop();
            log.info("Stopped consumer: {}", container.getListenerId());
        });
        
        // Wait for in-flight requests to complete
        log.info("Waiting for in-flight requests...");
        // Spring handles this automatically with graceful shutdown
    }
}
```

---

## Temat 48: Blue-Green & Canary Deployment

### 48.1 Zero-Downtime Deployment

**Problem:** Jak deploy'ować bez przerwy w działaniu?

**Rozwiązania:**
1. **Blue-Green** - Dwa środowiska, przełączanie ruchu
2. **Canary** - Stopniowe przekierowywanie ruchu (1% → 10% → 50% → 100%)

### 48.2 Blue-Green z Kubernetes

```yaml
# blue-deployment.yaml (current version)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventmaster-blue
  labels:
    version: blue
spec:
  replicas: 3
  selector:
    matchLabels:
      app: eventmaster
      version: blue
  template:
    metadata:
      labels:
        app: eventmaster
        version: blue
    spec:
      containers:
      - name: backend
        image: eventmaster:1.0.0

---
# green-deployment.yaml (new version)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventmaster-green
  labels:
    version: green
spec:
  replicas: 3
  selector:
    matchLabels:
      app: eventmaster
      version: green
  template:
    metadata:
      labels:
        app: eventmaster
        version: green
    spec:
      containers:
      - name: backend
        image: eventmaster:2.0.0

---
# service.yaml (points to blue initially)
apiVersion: v1
kind: Service
metadata:
  name: eventmaster
spec:
  selector:
    app: eventmaster
    version: blue  # Switch to 'green' when ready!
  ports:
  - port: 80
    targetPort: 8080
```

**Deployment process:**
```bash
# 1. Deploy green (new version)
kubectl apply -f green-deployment.yaml

# 2. Wait for green to be ready
kubectl rollout status deployment/eventmaster-green

# 3. Test green manually
kubectl port-forward svc/eventmaster-green 8081:8080
curl http://localhost:8081/actuator/health

# 4. Switch traffic to green
kubectl patch service eventmaster -p '{"spec":{"selector":{"version":"green"}}}'

# 5. Monitor for errors

# 6. If OK, delete blue
kubectl delete deployment eventmaster-blue

# 7. If ERROR, rollback!
kubectl patch service eventmaster -p '{"spec":{"selector":{"version":"blue"}}}'
```

### 48.3 Canary Deployment z Istio

```yaml
# virtual-service.yaml
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: eventmaster
spec:
  hosts:
  - eventmaster
  http:
  - match:
    - headers:
        canary:
          exact: "true"
    route:
    - destination:
        host: eventmaster
        subset: canary
  - route:
    - destination:
        host: eventmaster
        subset: stable
      weight: 90
    - destination:
        host: eventmaster
        subset: canary
      weight: 10  # 10% traffic to canary

---
# destination-rule.yaml
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: eventmaster
spec:
  host: eventmaster
  subsets:
  - name: stable
    labels:
      version: v1
  - name: canary
    labels:
      version: v2
```

**Gradual rollout:**
```bash
# 10% canary
kubectl apply -f canary-10.yaml

# Monitor metrics
watch "curl -s http://prometheus:9090/api/v1/query?query=rate(http_requests_total{version=\"v2\"}[5m])"

# If OK, increase to 50%
kubectl apply -f canary-50.yaml

# If still OK, 100%
kubectl apply -f canary-100.yaml
```

### 48.4 Database Migrations in Blue-Green

**Problem:** Blue i Green używają tej samej bazy!

**Rozwiązanie:** Backward-compatible migrations

```sql
-- ❌ BAD: Breaking change
ALTER TABLE events DROP COLUMN old_field;

-- ✅ GOOD: Backward compatible
-- Step 1 (deploy with green): Add new field
ALTER TABLE events ADD COLUMN new_field VARCHAR(255);

-- Step 2 (after green is stable): Backfill data
UPDATE events SET new_field = old_field WHERE new_field IS NULL;

-- Step 3 (next deployment): Drop old field
ALTER TABLE events DROP COLUMN old_field;
```

---

## Temat 49: Database Migrations

### 49.1 Flyway Setup

**pom.xml:**
```xml
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-core</artifactId>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-database-postgresql</artifactId>
</dependency>
```

**application.properties:**
```properties
spring.flyway.enabled=true
spring.flyway.locations=classpath:db/migration
spring.flyway.baseline-on-migrate=true
spring.flyway.validate-on-migrate=true
```

### 49.2 Migration Files

**Naming:** `V{version}__{description}.sql`

```sql
-- src/main/resources/db/migration/V1__initial_schema.sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    organizer_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version BIGINT DEFAULT 0
);

CREATE INDEX idx_events_organizer ON events(organizer_id);
CREATE INDEX idx_events_start_date ON events(start_date);
CREATE INDEX idx_events_status ON events(status);

-- src/main/resources/db/migration/V2__add_outbox_table.sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP
);

CREATE INDEX idx_outbox_published ON outbox(published, created_at);

-- src/main/resources/db/migration/V3__add_event_views.sql
CREATE TABLE event_views (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    organizer_id VARCHAR(255) NOT NULL,
    organizer_name VARCHAR(255),
    attendee_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_views_organizer ON event_views(organizer_id);
CREATE INDEX idx_event_views_start_date ON event_views(start_date);
```

### 49.3 Zero-Downtime Migrations

**Pattern:** Expand-Contract

```sql
-- V4__rename_column_step1_expand.sql
-- Add new column
ALTER TABLE events ADD COLUMN location_name VARCHAR(255);

-- Backfill from old column
UPDATE events SET location_name = location WHERE location_name IS NULL;

-- Application v2 writes to BOTH columns

-- V5__rename_column_step2_contract.sql (next release)
-- Drop old column
ALTER TABLE events DROP COLUMN location;
```

### 49.4 Rollback Strategy

**Flyway nie wspiera rollback!** Musisz ręcznie:

```sql
-- V6__add_category_field.sql
ALTER TABLE events ADD COLUMN category VARCHAR(100);

-- Rollback (manual):
-- R6__rollback_category_field.sql
ALTER TABLE events DROP COLUMN category;
```

**Better: Undo migrations manually jeśli potrzeba:**
```bash
flyway undo  # Requires Flyway Teams (paid)
```

---

## Temat 50: Production Checklist

### 50.1 Pre-Deployment Checklist

**Code Quality:**
- [ ] All tests passing (unit + integration)?
- [ ] Code review approved?
- [ ] No TODO/FIXME w critical paths?
- [ ] Security scan passed (Snyk, OWASP)?

**Database:**
- [ ] Migrations tested on staging?
- [ ] Backward compatible migrations?
- [ ] Indexes created CONCURRENTLY?
- [ ] Backup przed migracją?

**Configuration:**
- [ ] Secrets w Vault/K8s Secrets (nie w repo!)?
- [ ] Environment variables set correctly?
- [ ] Connection pools configured?
- [ ] Timeouts configured?

**Monitoring:**
- [ ] Metrics exposed?
- [ ] Logs structured (JSON)?
- [ ] Alerts configured?
- [ ] Dashboards ready?

**Performance:**
- [ ] Load tests passed?
- [ ] Database queries optimized?
- [ ] Caching configured?
- [ ] JVM tuned?

**High Availability:**
- [ ] Min 2 replicas?
- [ ] Health checks configured?
- [ ] Graceful shutdown enabled?
- [ ] Circuit breakers configured?

### 50.2 Post-Deployment Checklist

**Immediate (0-15min):**
- [ ] All pods healthy?
- [ ] No errors in logs?
- [ ] Response time < SLA?
- [ ] Error rate < 1%?

**Short-term (15min - 1h):**
- [ ] Database connections stable?
- [ ] Kafka lag normal?
- [ ] Memory not growing?
- [ ] CPU < 80%?

**Long-term (1h+):**
- [ ] No memory leaks?
- [ ] GC behavior normal?
- [ ] Alerts not firing?
- [ ] Business metrics OK?

### 50.3 Rollback Plan

```bash
# Kubernetes rollback
kubectl rollout undo deployment/eventmaster-backend

# Verify
kubectl rollout status deployment/eventmaster-backend

# If database migration ran, manual rollback needed!
psql -h localhost -U user -d eventmaster_db < rollback_v6.sql
```

### 50.4 Incident Response Runbook

**Scenario: High Error Rate**

1. **Detect:** Alert fires (error rate > 5%)
2. **Investigate:**
   ```bash
   # Check logs
   kubectl logs -l app=eventmaster --tail=100 | grep ERROR
   
   # Check metrics
   curl http://prometheus:9090/api/v1/query?query=rate(http_requests_total{status=~"5.."}[5m])
   
   # Check traces
   # Open Jaeger, filter by error=true
   ```

3. **Mitigate:**
   - Rollback if recent deployment
   - Scale up if resource issue
   - Disable feature flag if specific feature

4. **Resolve:**
   - Fix root cause
   - Deploy fix
   - Monitor

5. **Post-mortem:**
   - Document incident
   - Add tests to prevent recurrence
   - Update runbook

### 50.5 Security Checklist

**Application:**
- [ ] HTTPS only?
- [ ] JWT validation correct?
- [ ] SQL injection protected? (use PreparedStatement!)
- [ ] XSS protection enabled?
- [ ] CORS configured correctly?

**Infrastructure:**
- [ ] Secrets encrypted at rest?
- [ ] Network policies configured?
- [ ] Pod security policies enabled?
- [ ] Image scanning enabled?

**Dependencies:**
- [ ] No known vulnerabilities? (Snyk scan)
- [ ] Dependencies up to date?
- [ ] License compliance checked?

### 50.6 Backup & Recovery

**Database Backup:**
```bash
# Daily backup
pg_dump -h postgres -U user eventmaster_db | gzip > backup_$(date +%Y%m%d).sql.gz

# Upload to S3
aws s3 cp backup_$(date +%Y%m%d).sql.gz s3://eventmaster-backups/

# Test restore monthly!
gunzip < backup_20250120.sql.gz | psql -h localhost -U user eventmaster_test_db
```

**Kafka Backup:**
```bash
# MirrorMaker for disaster recovery
kafka-mirror-maker --consumer.config consumer.properties \
                   --producer.config producer.properties \
                   --whitelist="events.*"
```

### 50.7 Cost Optimization

**PostgreSQL:**
```sql
-- Archive old events
INSERT INTO events_archive SELECT * FROM events WHERE created_at < NOW() - INTERVAL '1 year';
DELETE FROM events WHERE created_at < NOW() - INTERVAL '1 year';
```

**Kafka:**
```properties
# Retention policy
log.retention.hours=168  # 7 days
log.retention.bytes=10737418240  # 10GB per partition
```

**Kubernetes:**
```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 2000m
    memory: 2Gi

# Horizontal Pod Autoscaler
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: eventmaster-backend
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: eventmaster-backend
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

---

## 🎉 GRATULACJE! CZĘŚĆ V UKOŃCZONA! 🎉

### Podsumowanie Części V:

**Nauczyłeś się:**
- ✅ **Testcontainers** - Testy integracyjne z prawdziwymi systemami
- ✅ **Contract Testing** - Pact dla API contracts
- ✅ **Chaos Engineering** - Testowanie odporności na awarie
- ✅ **Performance Testing** - Gatling, JVM tuning, Kafka optimization
- ✅ **Monitoring** - Metrics, Logs, Alerts
- ✅ **Distributed Tracing** - OpenTelemetry + Jaeger
- ✅ **Health Checks** - Liveness, Readiness, Graceful Shutdown
- ✅ **Zero-Downtime Deployment** - Blue-Green, Canary
- ✅ **Database Migrations** - Flyway, zero-downtime patterns
- ✅ **Production Readiness** - Checklist, runbooks, security

### 🚀 Co Dalej?

**EventMaster jest gotowy do produkcji!**

Masz teraz kompletną wiedzę o:
1. **CZĘŚĆ I:** Fundamenty Message & Event-Driven Architecture
2. **CZĘŚĆ II:** CQRS w praktyce
3. **CZĘŚĆ III:** Event Sourcing
4. **CZĘŚĆ IV:** Messaging & Kafka
5. **CZĘŚĆ V:** Testowanie & Operacje ← **Jesteś tutaj!**

### 📚 Rekomendowane Książki:

1. **"Implementing Domain-Driven Design"** - Vaughn Vernon
2. **"Building Microservices"** - Sam Newman
3. **"Designing Data-Intensive Applications"** - Martin Kleppmann
4. **"Site Reliability Engineering"** - Google
5. **"Release It!"** - Michael Nygard

### 🛠️ EventMaster - Twój Serwer Aplikacji:

**Spring Boot 3.4** to Twój serwer aplikacji w EventMaster:
- Embedded Tomcat (domyślnie)
- Obsługa HTTP/REST API
- Dependency Injection (Spring IoC)
- Transaction management
- JPA/Hibernate integration
- Kafka integration (Spring Kafka)
- Security (Spring Security + JWT)
- Actuator (metrics, health checks)

**Alternatywy:**
- Netty (reactive, Spring WebFlux)
- Undertow (lżejszy niż Tomcat)
- Jetty (tradycyjny servlet container)

Ale Spring Boot z Tomcat to **standard** dla większości aplikacji enterprise.

---

**Dokument ukończony:** 2025-01-20  
**Wersja:** 1.0  
**Status:** ✅ KOMPLETNA - WSZYSTKIE 50 TEMATÓW!  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4 + Nuxt 3  
**Autorzy:** EventMaster Architecture Team

**Powodzenia w budowie EventMaster! 🎊**
