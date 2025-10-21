# 🎓 EventMaster - Ścieżka Nauki: CQRS & Event-Driven Architecture

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack Technologiczny:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4 + Nuxt 3  
**Dla kogo:** Testera automatycznego chcącego opanować systemy rozproszone  
**Czas nauki:** 12-16 tygodni (50 szczegółowych tematów)  
**Status:** ✅ **KOMPLETNE** - Wszystkie 5 części ukończone (50 tematów)

---

## 📖 O Tej Ścieżce

To **kompleksowe opracowanie** architektury systemów rozproszonych oparte na **EventMaster** - rzeczywistym projekcie systemu zarządzania wydarzeniami. Dokumentacja zawiera:

- **15,000+ linii** szczegółowych wyjaśnień
- **Praktyczne przykłady kodu** (Spring Boot 3.4 + Apache Kafka 7.6)
- **Testy integracyjne** z Testcontainers
- **Docker Compose** konfiguracje (PostgreSQL 18, Kafka, Zookeeper)
- **Ćwiczenia praktyczne** do samodzielnej realizacji
- **Pułapki i best practices** dla testerów
- **Production-ready patterns** - Monitoring, Tracing, Deployment

---

## 🗺️ Struktura Dokumentacji

### 📘 [CZĘŚĆ I: FUNDAMENTY](./CZESC_I_FUNDAMENTY.md) (3,788 linii)
**Podstawy Message & Event-Driven Architecture**

#### Tematy 1-10:

1. **Message-Driven Architecture** - asynchroniczna komunikacja przez wiadomości
   - Producer/Consumer pattern
   - Apache Kafka basics
   - At-least-once delivery
   - Testcontainers dla Kafka

2. **Event-Driven Architecture (EDA)** - komunikacja przez zdarzenia (fakty)
   - Commands vs Events
   - Multiple listeners
   - Event broadcasting
   - Eventual consistency

3. **CQRS** - rozdzielenie zapisu i odczytu
   - Write Model vs Read Model
   - Projections
   - Denormalizacja
   - Performance optimization

4. **Commands vs Events vs Queries** - różnice i zastosowania
   - Nazewnictwo (CreateEvent vs EventCreated)
   - Validation (Commands tak, Events nie)
   - Immutability
   - Routing patterns

5. **Bounded Context** - granice modeli domenowych
   - Event Management Context
   - Ticketing Context
   - Analytics Context
   - Cross-context communication

6. **Aggregate** - granica spójności transakcyjnej
   - Aggregate Root
   - Business invariants
   - Transaction boundaries
   - Repository pattern

7. **Domain Events** - język biznesu w zdarzeniach
   - Ubiquitous Language
   - Event naming conventions
   - Event emission from Aggregates
   - Domain Event vs Integration Event

8. **Eventual Consistency** - spójność ostateczna
   - Strong Consistency vs Eventual
   - Testing async flows
   - Awaitility library
   - User experience considerations

9. **Idempotency** - odporność na duplikaty
   - At-least-once delivery problem
   - Idempotent handlers
   - ProcessedEvent table
   - Testing idempotency

10. **Saga Pattern** - długo działające transakcje
    - Orchestration vs Choreography
    - Compensation logic
    - Saga state management
    - Testing sagas

---

### 📗 [CZĘŚĆ II: CQRS W PRAKTYCE](./CZESC_II_CQRS_W_PRAKTYCE.md) (2,696 linii)
**Praktyczne implementacje wzorców CQRS**

#### Tematy 11-20:

11. **Projections** - budowanie Read Model
    - Simple projections (1:1)
    - Aggregating projections (N:1)
    - Error handling w projectorach
    - Monitoring metrics

12. **Multiple Read Models** - różne widoki dla różnych use cases
    - EventListView (szybkie listy)
    - EventDetailView (pełne szczegóły)
    - EventSearchView (full-text search)
    - PostgreSQL features (JSONB, arrays, tsvector)

13. **Rebuilding Projections** - odbudowa Read Model
    - Event Store implementation
    - Full rebuild strategy
    - Incremental rebuild
    - Admin REST API

14. **Optimistic Concurrency Control** - wykrywanie konfliktów
    - JPA @Version annotation
    - OptimisticLockException handling
    - Retry with exponential backoff
    - HTTP ETag headers

15. **Command Validation** - walidacja na 3 poziomach
    - Level 1: Syntax (Bean Validation)
    - Level 2: Semantic (Command logic)
    - Level 3: Business (Database checks)
    - Custom validators

16. **Command Handler Pattern** - obsługa komend
    - One handler per command type
    - Command Bus implementation
    - Transactional handling
    - Testing handlers

17. **Query Handler Pattern** - obsługa zapytań
    - One handler per query type
    - Query Bus implementation
    - Read-only operations
    - DTO mapping

18. **Denormalization** - duplikacja danych dla wydajności
    - Normalized vs Denormalized
    - When to denormalize
    - Keeping denormalized data in sync
    - Trade-offs

19. **Materialized Views** - PostgreSQL prekalkulowane widoki
    - CREATE MATERIALIZED VIEW
    - REFRESH strategies (scheduled, event-driven)
    - CONCURRENTLY option
    - Indexes on materialized views

20. **Snapshot Pattern** - optymalizacja odtwarzania
    - Snapshot every N events
    - Reconstruction optimization
    - Snapshot cleanup strategy
    - Testing snapshots

---

### 📕 [CZĘŚĆ III: EVENT SOURCING](./CZESC_III_EVENT_SOURCING.md) (2,961 linii)
**Zaawansowane wzorce Event Sourcing**

#### Tematy 21-30:

21. **Event Store Implementation** - persystencja eventów
22. **Event Versioning** - ewolucja schematów
23. **Temporal Queries** - zapytania "w czasie"
24. **Event Store Performance** - optymalizacja
25. **GDPR & Event Sourcing** - prawo do zapomnienia
26. **Event Store Debugging** - narzędzia diagnostyczne
27. **Hybrid Approaches** - CQRS bez Event Sourcing
28. **Event Store Schemas** - projektowanie tabel
29. **Event Replay** - odtwarzanie historii
30. **Event Sourcing Patterns** - zaawansowane wzorce

---

### 📙 [CZĘŚĆ IV: MESSAGING & KAFKA](./CZESC_IV_MESSAGING_KAFKA.md) (3,404 linie)
**Asynchroniczna komunikacja z Apache Kafka**

#### Tematy 31-40:

31. **Apache Kafka Architecture** - brokers, partitions, replicas
32. **Producers & Consumers** - implementacja w Spring
33. **Partitioning Strategies** - równomierny load balancing
34. **Consumer Groups** - paralelizacja konsumpcji
35. **Error Handling** - retry policies, Dead Letter Queue
36. **Exactly-Once Semantics** - transakcyjność w Kafka
37. **Schema Registry** - Avro schemas
38. **Kafka Streams** - stream processing
39. **Event Versioning** - backward/forward compatibility
40. **Transactional Outbox** - atomowość DB + Kafka

---

### 📘 [CZĘŚĆ V: TESTOWANIE & OPERACJE](./CZESC_V_TESTOWANIE_OPERACJE.md) (2,416 linii)
**Praktyczne aspekty dla testera i DevOps**

#### Tematy 41-50:

41. **Integration Testing with Testcontainers** - prawdziwe bazy i Kafka
42. **Contract Testing with Pact** - API contracts między frontend/backend
43. **Chaos Engineering** - Toxiproxy, Circuit Breakers, Resilience4j
44. **Performance Testing** - Gatling, JVM tuning, Kafka optimization
45. **Monitoring & Observability** - Metrics (Prometheus), Logs (ELK), Alerts
46. **Distributed Tracing** - OpenTelemetry + Jaeger
47. **Health Checks & Readiness** - Liveness/Readiness probes, Graceful shutdown
48. **Blue-Green & Canary Deployment** - Zero-downtime deployments
49. **Database Migrations** - Flyway, backward-compatible migrations
50. **Production Checklist** - Security, backups, incident response

---

### 📗 [CZĘŚĆ VI: TESTOWANIE PRAKTYCZNE PEŁNEGO STOSU](./CZESC_VI_TESTOWANIE_PRAKTYCZNE_STOSU.md) (Nowa!)
**Holistyczne testowanie każdego elementu EventMaster**

#### Lekcje 51-60:

51. **PostgreSQL 18 Features & Testing** - MERGE, SQL/JSON, wydajność, Flyway
52. **Keycloak OAuth2 Flow & Security Testing** - OAuth2/OIDC, JWT, RBAC
53. **Spring Boot Security Integration Testing** - Resource Server, method security
54. **Kafka Advanced Scenarios Testing** - Partitioning, rebalancing, exactly-once
55. **Nuxt 3 SSR & Frontend Integration Testing** - SSR/CSR, auth middleware, a11y
56. **Caddy Reverse Proxy & Routing Testing** - Load balancing, health checks, TLS
57. **Docker & Container Testing** - Compose, health checks, resource limits
58. **End-to-End Flow Testing (Full Stack)** - Complete user journey, eventual consistency
59. **Performance & Load Testing (Full Stack)** - Gatling, K6, bottleneck identification
60. **Production Readiness & Monitoring** - Prometheus, Grafana, Jaeger, alerting

📖 **[Quick Reference Guide](./CZESC_VI_QUICK_REFERENCE.md)** - Krótkie opisy wszystkich lekcji

---

## 🎯 Cele Nauki

Po ukończeniu pełnej ścieżki będziesz:

### Rozumieć:
- ✅ Fundamenty Message & Event-Driven Architecture
- ✅ Wzorzec CQRS i jego praktyczne zastosowania
- ✅ Event Sourcing i Event Store
- ✅ Bounded Contexts i Domain-Driven Design
- ✅ Eventual Consistency i jej konsekwencje

### Implementować:
- ✅ Projections i Multiple Read Models
- ✅ Command i Query Handlers
- ✅ Idempotentne event listeners
- ✅ Saga Pattern dla długo działających transakcji
- ✅ Optimistic Locking

### Testować:
- ✅ Asynchroniczne flows z Awaitility
- ✅ Integracje z Testcontainers (PostgreSQL + Kafka)
- ✅ Idempotentność handlers
- ✅ Eventual consistency
- ✅ Concurrent modifications

---

## 🛠️ Stack Technologiczny

### Backend
- **Java 21** - language features (records, pattern matching)
- **Spring Boot 3.4** - framework
- **PostgreSQL 18** - baza danych (Write + Read Models)
- **Apache Kafka 7.6** - event streaming (KRaft mode)
- **Flyway** - migracje bazy danych
- **Hibernate/JPA** - ORM z @Version

### Testing
- **JUnit 5** - unit tests
- **Testcontainers** - integration tests
- **Awaitility** - async testing
- **MockMvc** - REST API tests
- **EmbeddedKafka** - Kafka tests

### DevOps
- **Docker Compose** - local development
- **Kafka UI** - monitoring
- **Prometheus** - metrics
- **Grafana** - dashboards

### Frontend
- **Nuxt 3** - framework
- **TypeScript** - type safety
- **Pinia** - state management
- **TailwindCSS** - styling

---

## 📋 Jak Korzystać z Dokumentacji?

### Dla Początkujących:
1. **Zacznij od CZĘŚCI I** - przeczytaj wszystkie 10 tematów sekwencyjnie
2. **Eksperymentuj** - uruchom przykłady kodu lokalnie
3. **Wykonaj ćwiczenia** - praktyka czyni mistrza
4. **Przejdź do CZĘŚCI II** - gdy opanujesz fundamenty

### Dla Zaawansowanych:
1. **Skocz do interesujących tematów** - dokumentacja jest modułowa
2. **Porównaj z obecną implementacją** - dostosuj do swojego projektu
3. **Rozbuduj** - dodaj własne Read Models, Projections, etc.

### Dla Testerów:
1. **Skup się na sekcjach testowania** - każdy temat ma przykłady testów
2. **Testcontainers są kluczowe** - naucz się ich używać
3. **Awaitility** - opanuj testowanie asynchroniczne
4. **Idempotentność** - zawsze testuj!

---

## 🚀 Quick Start

### 1. Klonuj Repozytorium
```bash
git clone https://github.com/twoj-repo/eventmaster.git
cd eventmaster
```

### 2. Uruchom Infrastrukturę
```bash
cd docker
docker-compose up -d
```

To uruchomi:
- PostgreSQL 18 (port 5432)
- Apache Kafka (port 9092)
- Kafka UI (port 8090)

### 3. Uruchom Backend
```bash
cd backend
./mvnw spring-boot:run
```

### 4. Uruchom Frontend
```bash
cd frontend
npm install
npm run dev
```

### 5. Eksploruj
- Backend API: http://localhost:8080
- Frontend: http://localhost:3000
- Kafka UI: http://localhost:8090

---

## 📚 Dodatkowe Zasoby

### Książki (Zalecane):
1. **"Implementing Domain-Driven Design"** - Vaughn Vernon
2. **"Domain-Driven Design Distilled"** - Vaughn Vernon
3. **"Building Event-Driven Microservices"** - Adam Bellemare
4. **"Kafka: The Definitive Guide"** - Neha Narkhede et al.

### Artykuły:
- [Martin Fowler - CQRS](https://martinfowler.com/bliki/CQRS.html)
- [Event Sourcing - Greg Young](https://cqrs.files.wordpress.com/2010/11/cqrs_documents.pdf)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

### Projekty Referencyjne:
- [Axon Framework](https://axoniq.io/)
- [Eventuate](https://eventuate.io/)
- [Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream)

---

## 🎓 Plan Nauki (8-12 tygodni)

### Tydzień 1-2: Fundamenty (Tematy 1-5)
- Message-Driven Architecture
- Event-Driven Architecture
- CQRS basics
- Commands vs Events vs Queries
- Bounded Context

**Ćwiczenia:**
- Postaw lokalnie Kafka + PostgreSQL
- Zaimplementuj prosty Command → Event flow
- Napisz pierwszy Projector

### Tydzień 3-4: Zaawansowane Fundamenty (Tematy 6-10)
- Aggregate pattern
- Domain Events
- Eventual Consistency
- Idempotency
- Saga Pattern

**Ćwiczenia:**
- Zaimplementuj Aggregate z invariants
- Napisz testy z Awaitility
- Stwórz prostą Sagę

### Tydzień 5-6: CQRS w Praktyce Cz.1 (Tematy 11-15)
- Projections
- Multiple Read Models
- Rebuilding Projections
- Optimistic Concurrency Control
- Command Validation

**Ćwiczenia:**
- Stwórz 3 różne Read Models
- Zaimplementuj rebuilding mechanism
- Dodaj @Version do aggregates

### Tydzień 7-8: CQRS w Praktyce Cz.2 (Tematy 16-20)
- Command/Query Handler Patterns
- Denormalization
- Materialized Views
- Snapshot Pattern

**Ćwiczenia:**
- Zbuduj Command Bus
- Utwórz Materialized View w PostgreSQL
- Zaimplementuj Snapshot Pattern

### Tydzień 9-12: Projekt Końcowy
- Połącz wszystkie wzorce
- Zbuduj kompletny moduł EventMaster
- Napisz kompleksowe testy
- Wdróż lokalnie z Docker Compose

---

## 🔧 Wsparcie i Rozwój

### Masz pytania?
- Sprawdź sekcję **FAQ** w każdym temacie
- Zobacz **Pułapki dla Testera** w dokumentacji
- Przeanalizuj **przykłady testów**

### Znalazłeś błąd?
- Sprawdź obecną implementację w kodzie
- Zobacz sekcję **Troubleshooting**
- Zweryfikuj wersje zależności

### Chcesz przyczynić się?
- Dodaj własne przykłady
- Rozbuduj testy
- Udoskonalaj dokumentację

---

## 📊 Statystyki Dokumentacji

```
┌─────────────────────────────────────────────────┐
│  METRYKI ŚCIEŻKI NAUKI                          │
├─────────────────────────────────────────────────┤
│  Części:                                    5   │
│  Tematy szczegółowe:                       50   │
│  Łączna liczba linii:                  15,265   │
│  Przykłady kodu:                          600+  │
│  Testy przykładowe:                       400+  │
│  Diagramy:                                120+  │
│  Ćwiczenia praktyczne:                    100+  │
│  Production patterns:                      50+  │
└─────────────────────────────────────────────────┘
```

---

## ✅ Status Ukończenia

- ✅ **CZĘŚĆ I: Fundamenty** (Tematy 1-10) - **UKOŃCZONA**
- ✅ **CZĘŚĆ II: CQRS w Praktyce** (Tematy 11-20) - **UKOŃCZONA**
- ✅ **CZĘŚĆ III: Event Sourcing** (Tematy 21-30) - **UKOŃCZONA**
- ✅ **CZĘŚĆ IV: Messaging & Kafka** (Tematy 31-40) - **UKOŃCZONA**
- ✅ **CZĘŚĆ V: Testowanie & Operacje** (Tematy 41-50) - **UKOŃCZONA**
- 🎯 **CZĘŚĆ VI: Testowanie Praktyczne Stosu** (Lekcje 51-60) - **ZAPROJEKTOWANA** ⭐

**🎓 50 TEMATÓW UKOŃCZONE + 10 NOWYCH LEKCJI PRAKTYCZNYCH!**

---

## 🎉 Gratulacje!

Ukończyłeś kompleksową ścieżkę nauki o **CQRS i Event-Driven Architecture** w kontekście rzeczywistego projektu **EventMaster**.

**Masz teraz wiedzę do:**
- 🏗️ Projektowania systemów rozproszonych
- 📝 Implementacji CQRS i Event Sourcing
- 🔍 Testowania asynchronicznych flows
- 🚀 Wdrażania na produkcję

**Następne kroki:**
1. Zbuduj EventMaster od podstaw
2. Eksperymentuj z różnymi wzorcami
3. Dostosuj do własnych projektów
4. Podziel się wiedzą z zespołem

**Powodzenia! 🚀**

---

**Dokument utworzony:** 2025-01-20  
**Wersja:** 1.0  
**Status:** ✅ Kompletny  
**Stack:** PostgreSQL 18 + Apache Kafka 7.6 + Spring Boot 3.4 + Nuxt 3  
**Autorzy:** EventMaster Architecture Team
