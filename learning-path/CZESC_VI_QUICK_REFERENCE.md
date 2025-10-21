# 🎯 CZĘŚĆ VI - Quick Reference dla Testera

## 📚 10 Lekcji - Krótkie Opisy

### 🗺️ Mapa Zależności

```
PostgreSQL 18 (51)
    ↓
Keycloak Auth (52)
    ↓
Spring Security (53)
    ↓
Kafka Advanced (54)
    ↓
Nuxt 3 Frontend (55)
    ↓
Caddy Gateway (56)
    ↓
Docker Infra (57)
    ↓
E2E Full Stack (58)
    ↓
Performance Test (59)
    ↓
Production Ready (60)
```

---

## Lekcja 51: PostgreSQL 18 Features & Testing
**Focus:** Nowe features (MERGE, SQL/JSON), wydajność zapytań, transakcje, Flyway migrations  
**Narzędzia:** Testcontainers PostgreSQL 18, JdbcTemplate, EXPLAIN ANALYZE  
**Wiedza bazowa:** Write Model vs Read Model, ACID, Optimistic Locking  
**Ćwiczenia:** 5 (MERGE statement, deadlock, query optimization, migrations, pool exhaustion)

---

## Lekcja 52: Keycloak OAuth2 Flow & Security Testing
**Focus:** OAuth2/OIDC flows, JWT validation, RBAC, token management  
**Narzędzia:** Testcontainers Keycloak, Playwright, RestAssured  
**Wiedza bazowa:** Authorization Code Flow, JWT structure, Refresh tokens  
**Ćwiczenia:** 5 (E2E login, JWT validation, RBAC, token refresh, multi-client)

---

## Lekcja 53: Spring Boot Security Integration Testing
**Focus:** OAuth2 Resource Server, method security (@PreAuthorize), CORS, Actuator security  
**Narzędzia:** MockMvc, @WithMockJwt, RestAssured  
**Wiedza bazowa:** SecurityFilterChain, JwtAuthenticationConverter, role mapping  
**Ćwiczenia:** 5 (JWT auth, role-based authz, method security, CORS, Actuator endpoints)

---

## Lekcja 54: Kafka Advanced Scenarios Testing
**Focus:** Partitioning, consumer groups, rebalancing, exactly-once, schema evolution  
**Narzędzia:** EmbeddedKafka, Testcontainers Redpanda, Avro Schema Registry  
**Wiedza bazowa:** Partition keys, offset management, ISR, consumer lag  
**Ćwiczenia:** 5 (partition routing, rebalancing, exactly-once, schema evolution, lag monitoring)

---

## Lekcja 55: Nuxt 3 SSR & Frontend Integration Testing
**Focus:** SSR/CSR, auth middleware (@sidebase/nuxt-auth), API routes, E2E, accessibility  
**Narzędzia:** Playwright, Vitest, @nuxt/test-utils, axe-core  
**Wiedza bazowa:** Universal rendering, Nitro server, file-based routing  
**Ćwiczenia:** 5 (SSR rendering, auth middleware, API routes, component tests, a11y)

---

## Lekcja 56: Caddy Reverse Proxy & Routing Testing
**Focus:** Path-based routing, load balancing, health checks, TLS termination  
**Narzędzia:** curl, Gatling, Docker Compose, tcpdump  
**Wiedza bazowa:** Reverse proxy, header forwarding, upstream health checks  
**Ćwiczenia:** 5 (routing rules, header forwarding, load balancing, failover, TLS)

---

## Lekcja 57: Docker & Container Testing
**Focus:** Docker Compose orchestration, health checks, resource limits, networking, volumes  
**Narzędzia:** docker-compose, docker inspect, docker stats  
**Wiedza bazowa:** Images vs containers, multi-stage builds, depends_on  
**Ćwiczenia:** 5 (startup order, health checks, resource limits, networking, volumes)

---

## Lekcja 58: End-to-End Flow Testing (Full Stack)
**Focus:** Complete user journey, eventual consistency, multi-user scenarios, error handling  
**Narzędzia:** Playwright, Docker Compose, Page Object Model  
**Wiedza bazowa:** E2E philosophy, flaky tests, test data isolation  
**Ćwiczenia:** 5 (user journey, eventual consistency, multi-user, errors, performance)

---

## Lekcja 59: Performance & Load Testing (Full Stack)
**Focus:** Load testing, stress testing, bottleneck identification, profiling  
**Narzędzia:** Gatling, K6, JMeter, Prometheus, pg_stat_statements  
**Wiedza bazowa:** Load vs stress vs spike, performance targets, warm-up  
**Ćwiczenia:** 5 (read load test, write load test, stress test, query optimization, Kafka lag)

---

## Lekcja 60: Production Readiness & Monitoring
**Focus:** Observability (metrics, logs, traces), alerting, incident response, security hardening  
**Narzędzia:** Prometheus, Grafana, Jaeger, OpenTelemetry, ELK  
**Wiedza bazowa:** 3 pillars of observability, SLA/SLI/SLO, alert fatigue  
**Ćwiczenia:** 5 (metrics exposure, structured logs, distributed tracing, alerts, dashboards)

---

## 📊 Statystyki

| Metryka | Wartość |
|---------|---------|
| **Liczba lekcji** | 10 |
| **Ćwiczenia praktyczne** | 50 |
| **Technologie** | 10+ (PostgreSQL, Keycloak, Spring, Kafka, Nuxt, Caddy, Docker, Prometheus, Grafana, Jaeger) |
| **Narzędzia testowe** | 20+ (Testcontainers, Playwright, JUnit, MockMvc, K6, Gatling, etc.) |
| **Design patterns** | 25+ (Repository, Circuit Breaker, Bulkhead, Retry, etc.) |
| **Czas nauki** | 5-6 tygodni |

---

## 🎯 Kluczowe Umiejętności Po Ukończeniu

### Tester będzie potrafił:

**Warstwa Database (PostgreSQL 18):**
- ✅ Testować nowe features (MERGE, SQL/JSON)
- ✅ Optymalizować zapytania (EXPLAIN ANALYZE)
- ✅ Testować transakcje i concurrency
- ✅ Zarządzać migracjami (Flyway)

**Warstwa Security (Keycloak):**
- ✅ Testować OAuth2/OIDC flows
- ✅ Walidować JWT tokens
- ✅ Testować RBAC i uprawnienia
- ✅ Integrować z Spring Security

**Warstwa Backend (Spring Boot):**
- ✅ Testować REST API (MockMvc, RestAssured)
- ✅ Testować method security (@PreAuthorize)
- ✅ Konfigurować CORS
- ✅ Zabezpieczać Actuator endpoints

**Warstwa Messaging (Kafka):**
- ✅ Testować partitioning i consumer groups
- ✅ Obsługiwać rebalancing
- ✅ Implementować exactly-once semantics
- ✅ Zarządzać schema evolution

**Warstwa Frontend (Nuxt 3):**
- ✅ Testować SSR/CSR
- ✅ Testować auth middleware
- ✅ Testować API routes
- ✅ Testować accessibility (a11y)

**Warstwa Gateway (Caddy):**
- ✅ Testować routing rules
- ✅ Testować load balancing
- ✅ Testować health checks
- ✅ Konfigurować TLS

**Warstwa Infrastructure (Docker):**
- ✅ Orkiestrować Docker Compose
- ✅ Konfigurować health checks
- ✅ Zarządzać resource limits
- ✅ Testować networking

**Full Stack:**
- ✅ Testować E2E flows
- ✅ Testować performance (load, stress)
- ✅ Implementować observability
- ✅ Przygotować do produkcji

---

## 🛠️ Stack Technologiczny - Kompletny

### Application Stack
- **Frontend:** Nuxt 3.5+ (Vue 3, TypeScript)
- **Backend:** Spring Boot 3.4 (Java 21)
- **Database:** PostgreSQL 18
- **Messaging:** Apache Kafka 7.6 (KRaft mode)
- **Auth:** Keycloak (latest)
- **Gateway:** Caddy 2
- **Migrations:** Flyway

### Testing Stack
- **Unit:** JUnit 5, Vitest
- **Integration:** Testcontainers, @nuxt/test-utils
- **E2E:** Playwright, Selenium
- **API:** MockMvc, RestAssured, Supertest
- **Load:** Gatling, K6, JMeter
- **Contract:** Pact
- **Chaos:** Toxiproxy, Chaos Monkey
- **Security:** OWASP ZAP, SonarQube
- **Accessibility:** axe-core

### Observability Stack
- **Metrics:** Prometheus, Micrometer
- **Dashboards:** Grafana
- **Tracing:** Jaeger, OpenTelemetry
- **Logging:** ELK Stack (Elasticsearch, Logstash, Kibana)
- **APM:** Spring Boot Actuator

### Infrastructure Stack
- **Containers:** Docker, Docker Compose
- **Orchestration:** (Future: Kubernetes)
- **CI/CD:** (Future: GitHub Actions, Jenkins)
- **Service Mesh:** (Future: Istio)

---

## 📚 Zalecana Kolejność Nauki

### Faza 1: Podstawy (CZĘŚĆ I-V) - 8-12 tygodni
- ✅ CQRS & Event-Driven Architecture (50 tematów)
- ✅ Testcontainers basics
- ✅ Asynchroniczne testy (Awaitility)

### Faza 2: Testowanie Stosu (CZĘŚĆ VI) - 5-6 tygodni
- 🎯 Lekcja 51-60 (testowanie każdego elementu stosu)
- 🎯 Praktyczne implementacje testów
- 🎯 Observability i production readiness

### Faza 3: Zaawansowane (CZĘŚĆ VII) - Przyszłość
- ⏳ Microservices decomposition
- ⏳ Service mesh testing
- ⏳ Chaos engineering advanced
- ⏳ Multi-region deployment

---

## 🎓 Certyfikaty/Umiejętności Do Zdobycia

Po CZĘŚCI VI będziesz gotowy do:
- ✅ **ISTQB Test Automation Engineer**
- ✅ **AWS Certified Developer (testing perspective)**
- ✅ **Certified Kubernetes Application Developer (CKAD)**
- ✅ **Spring Professional Certification**
- ✅ **PostgreSQL Certified Professional**

---

## 🔗 Dodatkowe Zasoby

### Książki
1. "Testing Microservices with Mountebank" - Brandon Byars
2. "Continuous Delivery" - Jez Humble
3. "Release It!" - Michael Nygard
4. "Site Reliability Engineering" - Google
5. "Accelerate" - Nicole Forsgren

### Online Courses
1. Test Automation University (free)
2. Pluralsight: Testing in Spring Boot
3. Udemy: Kafka Testing Masterclass
4. Frontend Masters: Testing Web Applications

### Konferencje
1. Devoxx (Java/Spring)
2. Kafka Summit (Messaging)
3. Playwright Conf (E2E Testing)
4. SRECon (Production Engineering)
5. Spring One (Spring Ecosystem)

---

## ✅ Checklist Przed Rozpoczęciem

Upewnij się, że masz:
- [ ] Docker Desktop zainstalowany
- [ ] Java 21+ zainstalowany
- [ ] Node.js 20+ zainstalowany
- [ ] Maven/Gradle basics
- [ ] Git basics
- [ ] IDE (IntelliJ IDEA / VS Code)
- [ ] Podstawy SQL
- [ ] Podstawy HTTP/REST
- [ ] Ukończone CZĘŚĆ I-V (opcjonalne, ale zalecane)

---

## 🚀 Quick Start

```bash
# 1. Sklonuj repo
git clone <repo-url>
cd nuxt-learn

# 2. Uruchom infrastrukturę
cd docker
docker-compose up -d

# 3. Sprawdź, czy wszystko działa
docker-compose ps

# 4. Uruchom backend
cd ../backend
./mvnw spring-boot:run

# 5. Uruchom frontend
cd ../frontend
npm install
npm run dev

# 6. Otwórz przeglądarkę
# http://localhost (Caddy gateway)
# http://localhost:8080 (Backend API)
# http://localhost:3000 (Frontend direct)
# http://localhost:8180 (Keycloak admin)

# 7. Zacznij od Lekcji 51!
```

---

**Status:** ✅ GOTOWE DO NAUKI  
**Następny krok:** Rozpocznij od Lekcji 51 (PostgreSQL 18 Testing)  
**Wsparcie:** Sprawdź dokumentację w `learning-path/`
