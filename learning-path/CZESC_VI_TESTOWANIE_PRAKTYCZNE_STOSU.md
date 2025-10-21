# 🎓 CZĘŚĆ VI: TESTOWANIE PRAKTYCZNE PEŁNEGO STOSU (Lekcje 51-60)

**Projekt:** EventMaster (Rekonstrukcja 2025)  
**Stack:** PostgreSQL 18 + Kafka 7.6 + Keycloak + Caddy + Spring Boot 3.4 + Nuxt 3  
**Cel:** Holistyczne opanowanie testowania każdego elementu stosu technologicznego  
**Czas nauki:** 5-6 tygodni  
**Wymagania:** Ukończone CZĘŚĆ I-V (podstawy CQRS, Event-Driven Architecture, Testcontainers)

---

## 📋 Filozofia Części VI

Ta część różni się od poprzednich - zamiast uczyć się WZORCÓW (CQRS, Event Sourcing), uczymy się **JAK TESTOWAĆ KAŻDY ELEMENT STOSU**.

### Progresja wiedzy:
```
CZĘŚĆ I-V: Co to jest CQRS/EDA? → Jak działa Kafka? → Jak testować eventy?
          ↓
CZĘŚĆ VI:  Jak testować PostgreSQL 18? → Keycloak? → Caddy? → Serwer?
          ↓
EFEKT:    Holistyczna wiedza - "Widzę całość i wiem jak każda część się testuje"
```

### Kluczowe pytania, na które odpowiemy:
1. **PostgreSQL 18** - Jak testować nowe features (MERGE, SQL/JSON)? Jak testować wydajność zapytań?
2. **Keycloak** - Jak testować OAuth2 flow? Jak testować uprawnienia RBAC?
3. **Kafka** - Jak testować partycje? Consumer groups? Rebalancing?
4. **Caddy** - Jak testować reverse proxy? TLS? Load balancing?
5. **Spring Boot** - Jak testować Actuator? Metrics? Health checks?
6. **Nuxt 3** - Jak testować SSR? API routes? Auth middleware?
7. **Serwer** - Jak testować deployment? Docker? Resource limits?
8. **Integracja** - Jak testować wszystko razem? Contract testing? E2E?

---

## 🗺️ Plan 10 Lekcji - Strategia Nauki

### 📊 Macierz Zależności Wiedzy

```
┌─────────────────────────────────────────────────────────────┐
│  Lekcja 51: PostgreSQL 18 Features & Testing                │
│  ↓ (Wiedza o bazie danych)                                  │
│  Lekcja 52: Keycloak OAuth2 Flow & Security Testing        │
│  ↓ (Wiedza o autentykacji)                                  │
│  Lekcja 53: Spring Boot Security Integration Testing       │
│  ↓ (Backend + Auth)                                         │
│  Lekcja 54: Kafka Advanced Scenarios Testing               │
│  ↓ (Messaging layer)                                        │
│  Lekcja 55: Nuxt 3 SSR & Frontend Integration Testing      │
│  ↓ (Frontend + Auth)                                        │
│  Lekcja 56: Caddy Reverse Proxy & Routing Testing          │
│  ↓ (Gateway layer)                                          │
│  Lekcja 57: Docker & Container Testing                     │
│  ↓ (Infrastructure)                                         │
│  Lekcja 58: End-to-End Flow Testing (Full Stack)           │
│  ↓ (Integracja wszystkiego)                                │
│  Lekcja 59: Performance & Load Testing (Full Stack)        │
│  ↓ (Wydajność całości)                                      │
│  Lekcja 60: Production Readiness & Monitoring              │
└─────────────────────────────────────────────────────────────┘
```

---

## 📚 LEKCJA 51: PostgreSQL 18 Features & Testing

### 🎯 Cel Lekcji
Opanowanie testowania PostgreSQL 18 w kontekście EventMaster: nowe features (MERGE, SQL/JSON), wydajność, transakcje, replikacja.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o PostgreSQL 18?

**1. Nowe Features PostgreSQL 18:**
- `MERGE` statement (SQL:2023 standard)
- Rozszerzone `SQL/JSON` functions
- Improved `EXPLAIN` output
- Better partition pruning
- Logical replication improvements

**2. EventMaster Specific:**
- Write Model: PostgreSQL (agregaty, transakcje)
- Read Model: (projektowany jako PostgreSQL lub CockroachDB)
- Flyway migrations
- Connection pooling (HikariCP)

**3. Testing Concerns:**
- Transakcje ACID
- Concurrent modifications (Optimistic Locking)
- Query performance (EXPLAIN ANALYZE)
- Migration rollbacks
- Deadlocks

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**51.1 PostgreSQL 18 MERGE Statement**
- Test: MERGE dla upsert operacji (Event aggregate)
- Assertion: Idempotentne zapisywanie eventów
- Tool: Testcontainers PostgreSQL 18

**51.2 Optimistic Locking**
- Test: Concurrent UPDATE na tym samym aggregate
- Assertion: OptimisticLockException dla version conflict
- Tool: JUnit 5 + CountDownLatch dla symulacji współbieżności

**51.3 Query Performance**
- Test: EXPLAIN ANALYZE dla query Read Model
- Assertion: Seq Scan → Index Scan after optimization
- Tool: JdbcTemplate + SQL parsing

**51.4 Flyway Migration Testing**
- Test: Migration up + down (rollback)
- Assertion: Schema consistency
- Tool: Flyway + Testcontainers

**51.5 Connection Pool Exhaustion**
- Test: 100 concurrent connections
- Assertion: HikariCP waiting queue, eventual recovery
- Tool: JMH + Gatling

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Repository Pattern** - abstrakcja dostępu do bazy
- **Unit of Work** - transakcje jako jednostka pracy
- **Optimistic Locking** - wersjonowanie rekordów
- **Query Object** - enkapsulacja zapytań

**Best Practices z Konferencji:**
- Devoxx 2024: "PostgreSQL 16 to 18 Migration Checklist"
- Spring One 2024: "Testing Databases with Testcontainers 2.0"
- PostgreSQL Conference 2024: "Benchmarking PostgreSQL 18 MERGE"

**Autorytety:**
- Bruce Momjian (PostgreSQL Core Team)
- Andres Freund (PostgreSQL Developer)
- Craig Kerstiens (PostgreSQL Expert)

### 📝 Ćwiczenia Praktyczne

1. **Implementuj MERGE dla EventAggregate**
2. **Napisz test dla deadlock scenario**
3. **Zmierz wydajność query przed i po indeksie**
4. **Stwórz Flyway migration z rollback**
5. **Przetestuj connection pool pod obciążeniem**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po opanowaniu PostgreSQL 18, przechodzimy do **Keycloak** (Lekcja 52), ponieważ:
- Keycloak używa PostgreSQL jako storage
- Testowanie OAuth2 flow wymaga zrozumienia transakcji
- Security testing bazuje na wiedzy o izolacji transakcji

---

## 📚 LEKCJA 52: Keycloak OAuth2 Flow & Security Testing

### 🎯 Cel Lekcji
Opanowanie testowania Keycloak w kontekście EventMaster: OAuth2/OIDC flow, RBAC, token validation, integration z Spring Security.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Keycloak?

**1. Keycloak Architecture:**
- Realms (izolacja tenant'ów)
- Clients (aplikacje: frontend, backend)
- Users, Groups, Roles
- Identity Providers (SSO)
- Token types: Access Token (JWT), Refresh Token, ID Token

**2. OAuth2/OIDC Flows:**
- **Authorization Code Flow** - dla SPA (Nuxt)
- **Client Credentials Flow** - dla backend-to-backend
- **Token Refresh Flow**
- **Logout Flow** (RP-Initiated Logout)

**3. EventMaster Integration:**
```
User → Nuxt (@sidebase/nuxt-auth) → Authorization Endpoint
                                    ↓
                            Keycloak (http://localhost:8180)
                                    ↓
                            Callback (/api/auth/callback)
                                    ↓
                            Access Token (JWT)
                                    ↓
                            API Call → Spring Boot (OAuth2 Resource Server)
                                    ↓
                            JWT Validation (JWK Set)
```

**4. Testing Concerns:**
- Token expiration & refresh
- RBAC (Role-Based Access Control)
- CSRF protection
- Invalid tokens (expired, malformed)
- Multi-tenant scenarios

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**52.1 Authorization Code Flow (E2E)**
- Test: User login → Authorization → Callback → Access Token
- Assertion: Token contains correct roles
- Tool: Playwright + Testcontainers Keycloak

**52.2 JWT Validation w Spring Boot**
- Test: Request z valid/invalid JWT
- Assertion: 200 OK vs 401 Unauthorized
- Tool: RestAssured + MockMvc

**52.3 RBAC - Role Enforcement**
- Test: User z rolą "USER" próbuje admin endpoint
- Assertion: 403 Forbidden
- Tool: @WithMockUser + @PreAuthorize

**52.4 Token Refresh Scenario**
- Test: Access Token expires → Auto-refresh → Request succeeds
- Assertion: Seamless user experience
- Tool: Nuxt auth interceptor test

**52.5 Multi-Client Scenario**
- Test: Frontend client + Backend client (different scopes)
- Assertion: Backend może operować w imieniu serwisu
- Tool: Keycloak Admin API

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Gateway Pattern** - Keycloak jako Authentication Gateway
- **Token Introspection** - walidacja tokena
- **Service Account** - backend-to-backend auth
- **Scope-Based Authorization** - fine-grained permissions

**Best Practices z Konferencji:**
- DevNexus 2024: "OAuth2 Security Pitfalls"
- Devoxx 2024: "Keycloak 24.0 Best Practices"
- OWASP Summit 2024: "Testing OAuth2 Implementations"

**Autorytety:**
- Stian Thorgersen (Keycloak Creator, Red Hat)
- Philippe De Ryck (OAuth2 Security Expert)
- Aaron Parecki (OAuth2 in Action author)

### 📝 Ćwiczenia Praktyczne

1. **Skonfiguruj Keycloak Realm dla EventMaster**
2. **Napisz test E2E dla login flow**
3. **Przetestuj expired token scenario**
4. **Zaimplementuj role-based endpoint protection**
5. **Testuj token refresh mechanism**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po opanowaniu Keycloak (auth layer), przechodzimy do **Spring Boot Security Integration** (Lekcja 53):
- Integracja Keycloak z Spring Security
- Resource Server configuration
- Method-level security

---

## 📚 LEKCJA 53: Spring Boot Security Integration Testing

### 🎯 Cel Lekcji
Testowanie integracji Spring Boot 3.4 z Keycloak: OAuth2 Resource Server, method security, Actuator security, CORS.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Spring Security?

**1. Spring Security Architecture:**
- SecurityFilterChain
- OAuth2ResourceServerConfigurer
- JwtAuthenticationConverter
- GrantedAuthority (role mapping)

**2. EventMaster Security Config:**
```java
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {
    
    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) {
        return http
            .oauth2ResourceServer(oauth2 -> oauth2.jwt())
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/events/**").hasRole("USER")
                .requestMatchers("/actuator/**").hasRole("ADMIN")
            )
            .build();
    }
}
```

**3. Method Security:**
```java
@PreAuthorize("hasRole('ADMIN')")
public void deleteEvent(UUID eventId) { ... }

@PostAuthorize("returnObject.createdBy == authentication.name")
public Event getEvent(UUID eventId) { ... }
```

**4. Testing Concerns:**
- JWT-based authentication
- Role mapping (Keycloak roles → Spring Security authorities)
- CORS configuration (Caddy → Spring Boot)
- Actuator security
- Exception handling (401, 403)

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**53.1 JWT Authentication**
- Test: Request z valid JWT → 200 OK
- Test: Request bez JWT → 401 Unauthorized
- Tool: @SpringBootTest + MockMvc + @WithMockJwt

**53.2 Role-Based Authorization**
- Test: USER role → access /api/events → 200
- Test: USER role → access /actuator/metrics → 403
- Tool: @WithMockUser(roles={"USER"})

**53.3 Method Security (@PreAuthorize)**
- Test: Admin user calls deleteEvent() → success
- Test: Regular user calls deleteEvent() → AccessDeniedException
- Tool: @WithMockUser + assertThrows

**53.4 CORS Configuration**
- Test: OPTIONS preflight request from http://localhost
- Assertion: Access-Control-Allow-Origin header present
- Tool: MockMvc + CORS matcher

**53.5 Actuator Endpoints Security**
- Test: /actuator/health (public) → 200
- Test: /actuator/metrics (secured) → 401 without auth
- Tool: RestAssured

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Filter Chain Pattern** - Spring Security filters
- **Strategy Pattern** - Authentication providers
- **Decorator Pattern** - Security method proxies
- **Template Method** - AbstractSecurityInterceptor

**Best Practices z Konferencji:**
- Spring One 2024: "Spring Security 6.3 New Features"
- JavaOne 2024: "Testing Security with Spring Boot 3"
- OWASP AppSec 2024: "Securing REST APIs"

**Autorytety:**
- Rob Winch (Spring Security Lead)
- Josh Long (Spring Developer Advocate)
- Daniel Garnier-Moiroux (Spring Security Contributor)

### 📝 Ćwiczenia Praktyczne

1. **Implementuj JwtAuthenticationConverter z Keycloak roles**
2. **Napisz test dla każdego secured endpoint**
3. **Przetestuj CORS dla różnych origins**
4. **Zabezpiecz Actuator endpoints role-based**
5. **Zaimplementuj custom AccessDeniedHandler**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po zabezpieczeniu backendu, przechodzimy do **Kafka Advanced Scenarios** (Lekcja 54):
- Security w Kafka (SASL/SSL)
- Authorization (ACLs)
- Testowanie produkcji/konsumpcji eventów z secured context

---

## 📚 LEKCJA 54: Kafka Advanced Scenarios Testing

### 🎯 Cel Lekcji
Zaawansowane testowanie Kafka: partitioning, consumer groups, rebalancing, exactly-once semantics, schema evolution.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Kafka (zaawansowane)?

**1. Kafka Deep Dive:**
- Partitions & Partition Keys
- Consumer Groups & Rebalancing
- Offset Management (auto-commit vs manual)
- Message Ordering (per-partition)
- ISR (In-Sync Replicas)

**2. EventMaster Kafka Topics:**
```
commands.events.create          (1 partition, key: eventId)
domain.events.lifecycle         (3 partitions, key: eventId)
integration.events.published    (1 partition, key: eventId)
dead-letter-queue              (1 partition)
```

**3. Consumer Groups:**
- `eventmaster-command-handlers` (1 instance - ordering)
- `eventmaster-projectors` (3 instances - parallel)
- `eventmaster-notifications` (1 instance)

**4. Testing Concerns:**
- Message ordering (w ramach partycji)
- Consumer rebalancing (dodanie/usunięcie instance)
- Duplicate messages (at-least-once delivery)
- Lost messages (acks configuration)
- Slow consumers (lag monitoring)

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**54.1 Partition-Key Routing**
- Test: 100 eventów z tym samym eventId → ta sama partycja
- Assertion: Ordering preserved w ramach eventId
- Tool: EmbeddedKafka + ConsumerRecordCaptor

**54.2 Consumer Group Rebalancing**
- Test: 3 consumers → stop 1 consumer → rebalance → continue
- Assertion: Wszystkie messages przetworzone, brak duplikatów
- Tool: @EmbeddedKafka + manual consumer management

**54.3 Exactly-Once Semantics**
- Test: Transactional producer + idempotent consumer
- Assertion: Message processed exactly once (DB + Kafka atomic)
- Tool: Transactional Outbox Pattern test

**54.4 Schema Evolution (Backward Compatibility)**
- Test: Producer V2 (new field) → Consumer V1 (old schema)
- Assertion: Consumer V1 ignores unknown field, no error
- Tool: Avro Schema Registry + backward compatibility check

**54.5 Consumer Lag Monitoring**
- Test: Slow consumer → lag increases → alert triggered
- Assertion: Prometheus metric `kafka_consumer_lag` > threshold
- Tool: Kafka consumer metrics + Micrometer

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Competing Consumers** - parallel processing
- **Message Router** - partition key based routing
- **Idempotent Receiver** - deduplikacja
- **Transactional Outbox** - atomicity DB + Kafka

**Best Practices z Konferencji:**
- Kafka Summit 2024: "Kafka 3.7 Performance Tuning"
- Devoxx 2024: "Testing Kafka Applications"
- QCon 2024: "Exactly-Once Semantics in Practice"

**Autorytety:**
- Jun Rao (Kafka Co-Creator, Confluent)
- Gwen Shapira (Kafka Expert, Confluent)
- Tim Berglund (Kafka Developer Advocate)

### 📝 Ćwiczenia Praktyczne

1. **Zaimplementuj partitioning strategy dla EventMaster**
2. **Napisz test dla consumer rebalancing**
3. **Przetestuj Transactional Outbox Pattern**
4. **Dodaj Schema Registry i test backward compatibility**
5. **Monitoruj consumer lag w Prometheus**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po Kafka (async backend layer), przechodzimy do **Nuxt 3 SSR & Frontend Testing** (Lekcja 55):
- Frontend konsumuje backend API (zabezpieczone Keycloak)
- SSR (Server-Side Rendering) vs CSR
- Testing Nuxt auth middleware

---

## 📚 LEKCJA 55: Nuxt 3 SSR & Frontend Integration Testing

### 🎯 Cel Lekcji
Testowanie Nuxt 3 aplikacji: SSR/CSR, auth middleware (@sidebase/nuxt-auth), API routes, Playwright E2E, accessibility.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Nuxt 3?

**1. Nuxt 3 Architecture:**
- **Universal Rendering** (SSR + CSR hybrid)
- **Server Engine** (Nitro) - API routes w `/server/api/`
- **Auto-imports** (composables, components)
- **File-based Routing** (`/pages/`)
- **Middleware** (global, route-specific)

**2. EventMaster Frontend:**
```
/pages/index.vue                    → Strona główna (SSR)
/pages/events/index.vue             → Lista eventów (SSR)
/pages/events/[id].vue              → Szczegóły (SSR)
/pages/events/create.vue            → Tworzenie (CSR, auth required)

/server/api/auth/[...].ts           → Auth callback (Nuxt server)

Middleware:
  - auth.global.ts                  → Global auth check
```

**3. @sidebase/nuxt-auth Integration:**
- OAuth2 Authorization Code Flow
- Automatic token management
- Protected routes
- Session handling

**4. Testing Concerns:**
- SSR hydration errors
- Auth middleware protection
- API route testing
- Component testing (Vue Test Utils)
- E2E testing (Playwright)
- Accessibility (axe-core)

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**55.1 SSR Rendering**
- Test: Fetch `/events` → HTML response contains event list
- Assertion: SEO-friendly HTML (not blank page)
- Tool: Playwright + page.content()

**55.2 Auth Middleware**
- Test: Unauthenticated user → /events/create → redirect to login
- Assertion: URL contains Keycloak authorization endpoint
- Tool: Playwright + page.waitForURL()

**55.3 Protected API Route**
- Test: Call /api/auth/session without cookie → 401
- Assertion: Session API requires authentication
- Tool: Vitest + $fetch (Nuxt test utils)

**55.4 Component Testing (Event Card)**
- Test: Render EventCard with props
- Assertion: Displays title, date, location
- Tool: @nuxt/test-utils + @vue/test-utils

**55.5 Accessibility Testing**
- Test: Event list page → axe-core scan
- Assertion: No critical a11y violations
- Tool: Playwright + @axe-core/playwright

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **BFF (Backend for Frontend)** - Nuxt server API routes
- **Middleware Pattern** - auth guards
- **Composition API** - reusable composables
- **Lazy Loading** - code splitting

**Best Practices z Konferencji:**
- Vue.js Amsterdam 2024: "Testing Nuxt 3 Applications"
- JSNation 2024: "SSR Performance Optimization"
- Web Directions 2024: "Accessibility Testing in SPAs"

**Autorytety:**
- Daniel Roe (Nuxt Core Team)
- Alexander Lichter (Nuxt Contributor)
- Debbie O'Brien (Playwright Developer Advocate)

### 📝 Ćwiczenia Praktyczne

1. **Napisz Playwright test dla login flow**
2. **Przetestuj SSR hydration (no console errors)**
3. **Zaimplementuj unit test dla composable**
4. **Dodaj axe-core accessibility tests**
5. **Test auth middleware dla różnych routes**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po opanowaniu frontendu, przechodzimy do **Caddy Reverse Proxy** (Lekcja 56):
- Caddy jako gateway (routing: / → Nuxt, /api/v1 → Spring Boot)
- TLS termination
- Load balancing

---

## 📚 LEKCJA 56: Caddy Reverse Proxy & Routing Testing

### 🎯 Cel Lekcji
Testowanie Caddy jako API Gateway: reverse proxy routing, TLS, load balancing, health checks, rate limiting.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Caddy?

**1. Caddy Architecture:**
- Automatic HTTPS (Let's Encrypt)
- Reverse Proxy (`reverse_proxy` directive)
- Request Matching (path-based routing)
- Load Balancing (round-robin, least_conn)
- Health Checks (active/passive)

**2. EventMaster Caddyfile:**
```caddyfile
http://localhost {
    # Backend API
    reverse_proxy /api/v1/* http://host.docker.internal:8080
    
    # Nuxt SSR + Auth callback
    reverse_proxy /api/auth/* http://host.docker.internal:3000
    
    # Frontend (catch-all)
    reverse_proxy * http://host.docker.internal:3000
}
```

**3. Routing Logic:**
```
Client Request:
  - /api/v1/events           → Spring Boot :8080
  - /api/auth/callback       → Nuxt Server :3000 (OIDC callback)
  - /                        → Nuxt :3000 (frontend)
  - /events/123              → Nuxt :3000 (frontend)
```

**4. Testing Concerns:**
- Routing correctness (path matching)
- Header forwarding (Host, X-Forwarded-For)
- CORS headers
- Load balancing (multiple backend instances)
- TLS configuration
- Upstream health checks

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**56.1 Path-Based Routing**
- Test: GET http://localhost/api/v1/events → routed to :8080
- Assertion: Response from Spring Boot backend
- Tool: curl + Docker Compose

**56.2 Header Forwarding**
- Test: Request → Caddy → Backend
- Assertion: `X-Forwarded-For`, `X-Forwarded-Proto` headers present
- Tool: MockMvc + header assertions

**56.3 Load Balancing (Multi-Backend)**
- Test: 100 requests → 2 backend instances
- Assertion: ~50 requests per instance (round-robin)
- Tool: Gatling + backend request counter

**56.4 Health Check Failover**
- Test: Backend :8080 down → Caddy marks as unhealthy
- Assertion: Requests routed to :8081 (backup instance)
- Tool: Docker stop + curl

**56.5 TLS Termination**
- Test: HTTPS request → Caddy terminates TLS → HTTP to backend
- Assertion: Backend receives HTTP request
- Tool: openssl s_client + tcpdump

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **API Gateway Pattern** - single entry point
- **BFF Pattern** - backend routing
- **Circuit Breaker** - health check failover
- **Rate Limiting** - DDoS protection

**Best Practices z Konferencji:**
- NGINX Conf 2024: "Modern API Gateway Patterns"
- KubeCon 2024: "Service Mesh vs API Gateway"
- QCon 2024: "Zero-Downtime Deployments"

**Autorytety:**
- Matt Holt (Caddy Creator)
- Kelsey Hightower (Infrastructure Expert)
- Sam Newman (Microservices Author)

### 📝 Ćwiczenia Praktyczne

1. **Napisz test dla każdej reguły routingu w Caddyfile**
2. **Przetestuj failover scenario (backend down)**
3. **Dodaj rate limiting i test overload**
4. **Konfiguruj TLS i test HTTPS**
5. **Load balancing test z 3 backend instances**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po Caddy (infrastructure layer), przechodzimy do **Docker & Container Testing** (Lekcja 57):
- Docker Compose orchestration
- Health checks w Docker
- Resource limits (CPU, memory)
- Testing w kontenerach

---

## 📚 LEKCJA 57: Docker & Container Testing

### 🎯 Cel Lekcji
Testowanie aplikacji w kontenerach Docker: Docker Compose, health checks, resource limits, networking, volumes, multi-stage builds.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Docker?

**1. Docker Fundamentals:**
- Images vs Containers
- Dockerfile (multi-stage builds)
- Docker Compose (orchestration)
- Networks (bridge, host)
- Volumes (data persistence)

**2. EventMaster Docker Setup:**
```yaml
services:
  postgres:
    image: postgres:18
    healthcheck:
      test: ["CMD-SHELL", "pg_isready"]
      interval: 10s
  
  keycloak:
    image: quay.io/keycloak/keycloak:latest
    depends_on:
      postgres: { condition: service_healthy }
  
  backend:
    build: ./backend
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/...
  
  frontend:
    build: ./frontend
    depends_on:
      - backend
  
  caddy:
    image: caddy:latest
    ports:
      - "80:80"
```

**3. Health Checks:**
- Startup health check (application ready?)
- Liveness probe (application alive?)
- Readiness probe (application can handle traffic?)

**4. Testing Concerns:**
- Container startup order (depends_on)
- Health check failures
- Network connectivity (service discovery)
- Resource limits (OOMKilled)
- Volume mounts (data persistence)
- Multi-stage build optimization

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**57.1 Docker Compose Startup Order**
- Test: docker-compose up → services start in correct order
- Assertion: Backend waits for Postgres health check
- Tool: docker-compose + healthcheck logs

**57.2 Health Check Configuration**
- Test: Backend container with broken DB connection
- Assertion: Health check fails, container marked unhealthy
- Tool: docker inspect + health status

**57.3 Resource Limits**
- Test: Backend with memory limit 512MB
- Assertion: Container OOMKilled when exceeding limit
- Tool: docker stats + memory stress test

**57.4 Network Connectivity**
- Test: Frontend → backend (internal network)
- Assertion: Service discovery works (DNS resolution)
- Tool: docker exec + curl from container

**57.5 Volume Persistence**
- Test: Postgres data volume → docker-compose down → up
- Assertion: Data persists across container restarts
- Tool: psql + data verification

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Sidecar Pattern** - helper containers
- **Ambassador Pattern** - proxy containers
- **Init Container** - setup before main app
- **Blue-Green Deployment** - zero-downtime

**Best Practices z Konferencji:**
- DockerCon 2024: "Multi-Stage Build Best Practices"
- KubeCon 2024: "Health Checks & Readiness Probes"
- DevOps Days 2024: "Container Security Scanning"

**Autorytety:**
- Jérôme Petazzoni (Docker Contributor)
- Jessie Frazelle (Container Security Expert)
- Kelsey Hightower (Kubernetes Advocate)

### 📝 Ćwiczenia Praktyczne

1. **Zoptymalizuj Dockerfile (multi-stage build)**
2. **Dodaj health checks do wszystkich serwisów**
3. **Przetestuj resource limits (CPU, memory)**
4. **Testuj network connectivity między kontenerami**
5. **Weryfikuj volume persistence**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po Docker (container layer), przechodzimy do **End-to-End Flow Testing** (Lekcja 58):
- Cały stack w Docker Compose
- E2E test flow: Login → Create Event → Verify
- Integration wszystkich komponentów

---

## 📚 LEKCJA 58: End-to-End Flow Testing (Full Stack)

### 🎯 Cel Lekcji
Testowanie pełnego flow aplikacji EventMaster: od logowania użytkownika przez Keycloak, tworzenie eventu, aż po weryfikację w bazie i projekcji.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o E2E Testing?

**1. E2E Testing Philosophy:**
- Testowanie z perspektywy użytkownika
- Wszystkie komponenty działające razem (nie mocks!)
- Wolniejsze, ale najbardziej realistyczne
- Flaky tests problem (timing, network)

**2. EventMaster E2E Flow:**
```
1. User Login (Keycloak)
   Browser → http://localhost → Caddy → Nuxt (SSR)
   ↓
   Redirect → Keycloak (/realms/eventmaster/protocol/openid-connect/auth)
   ↓
   Login form → username/password
   ↓
   Callback → /api/auth/callback (Nuxt server)
   ↓
   JWT Token stored in session

2. Create Event
   Browser → POST /events (Nuxt composable)
   ↓
   Caddy → Spring Boot /api/v1/events
   ↓
   Command published to Kafka (commands.events.create)
   ↓
   CommandHandler processes
   ↓
   EventCreated domain event published
   ↓
   Projector updates Read Model (PostgreSQL)

3. Verify Event
   Browser → GET /events/:id (Nuxt SSR)
   ↓
   Spring Boot → Read Model query
   ↓
   Event displayed in UI
```

**3. Testing Concerns:**
- Full stack running (Docker Compose)
- Timing issues (eventual consistency)
- Test data isolation
- Flaky tests (network, timing)
- Screenshot/video on failure

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**58.1 Complete User Journey**
- Test: Login → Create Event → View Event → Logout
- Assertion: Event persisted in DB, visible in UI
- Tool: Playwright + Docker Compose

**58.2 Eventual Consistency E2E**
- Test: Create Event → Wait → Projection updated
- Assertion: Event visible in list within 5 seconds
- Tool: Playwright + polling/retry

**58.3 Multi-User Scenario**
- Test: User A creates event → User B sees it
- Assertion: Read Model updated for all users
- Tool: Playwright (2 browser contexts)

**58.4 Error Handling E2E**
- Test: Create Event with invalid data → Error message displayed
- Assertion: User-friendly error, no crash
- Tool: Playwright + expect(page.locator('.error'))

**58.5 Performance E2E**
- Test: Create Event → measure end-to-end latency
- Assertion: < 3 seconds (p95)
- Tool: Playwright + performance metrics

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Page Object Model** - abstrakcja stron w testach
- **Screenplay Pattern** - user-centric test scenarios
- **Test Data Builder** - fluent test data creation
- **Retry Pattern** - eventual consistency handling

**Best Practices z Konferencji:**
- Selenium Conf 2024: "Flaky Tests Elimination"
- Playwright Conf 2024: "Modern E2E Testing"
- TestJS Summit 2024: "Visual Regression Testing"

**Autorytety:**
- Angie Jones (Test Automation Expert)
- Gleb Bahmutov (Cypress Creator)
- Debbie O'Brien (Playwright Developer Advocate)

### 📝 Ćwiczenia Praktyczne

1. **Napisz E2E test dla complete user journey**
2. **Implementuj Page Object Model dla EventMaster**
3. **Dodaj retry logic dla eventual consistency**
4. **Przetestuj error scenarios E2E**
5. **Zmierz end-to-end latency (create → view)**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po E2E testing (functional correctness), przechodzimy do **Performance & Load Testing** (Lekcja 59):
- Load testing (Gatling, K6)
- Stress testing (breaking point)
- Bottleneck identification

---

## 📚 LEKCJA 59: Performance & Load Testing (Full Stack)

### 🎯 Cel Lekcji
Testowanie wydajności i obciążenia pełnego stosu EventMaster: load testing (Gatling/K6), profiling (JVM, PostgreSQL), bottleneck identification.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Performance Testing?

**1. Performance Testing Types:**
- **Load Testing** - zachowanie pod normalnym obciążeniem
- **Stress Testing** - zachowanie pod ekstremalnym obciążeniem
- **Spike Testing** - nagły wzrost ruchu
- **Soak Testing** - długotrwałe obciążenie (memory leaks)
- **Scalability Testing** - horizontal/vertical scaling

**2. EventMaster Performance Targets:**
```
┌─────────────────────────────────────────────────┐
│  Endpoint              │ p50   │ p95   │ p99    │
├─────────────────────────────────────────────────┤
│  GET /events           │ 50ms  │ 150ms │ 300ms  │
│  GET /events/:id       │ 30ms  │ 100ms │ 200ms  │
│  POST /events          │ 100ms │ 500ms │ 1s     │
│  Event Processing      │ 80ms  │ 300ms │ 600ms  │
└─────────────────────────────────────────────────┘

Throughput:
  - 1000 req/s (read endpoints)
  - 100 req/s (write endpoints)
  - 500 events/s (Kafka processing)
```

**3. Bottleneck Suspects:**
- Database (slow queries, missing indexes)
- Kafka (consumer lag, partition bottleneck)
- CPU (algorithmic complexity)
- Memory (GC pauses, leaks)
- Network (latency, bandwidth)
- Connection pools (exhaustion)

**4. Testing Concerns:**
- Realistic load patterns
- Warm-up period (JIT compilation)
- Monitoring during test (metrics)
- Bottleneck identification
- Resource utilization (CPU, memory, disk)

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**59.1 Load Test (Read Endpoints)**
- Test: 1000 req/s GET /events for 10 minutes
- Assertion: p95 < 150ms, error rate < 0.1%
- Tool: Gatling / K6

**59.2 Load Test (Write Endpoints)**
- Test: 100 req/s POST /events for 5 minutes
- Assertion: p99 < 1s, all events processed
- Tool: K6 + Prometheus

**59.3 Stress Test (Breaking Point)**
- Test: Gradually increase load until failure
- Assertion: Identify max throughput, failure mode
- Tool: Gatling (ramp-up scenario)

**59.4 Database Query Performance**
- Test: EXPLAIN ANALYZE for slow queries
- Assertion: No sequential scans on large tables
- Tool: PostgreSQL pg_stat_statements

**59.5 Kafka Consumer Lag**
- Test: Produce 10k events → measure consumer lag
- Assertion: Lag < 1000 messages, lag time < 10s
- Tool: Kafka metrics + Prometheus

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Bulkhead Pattern** - izolacja resource pools
- **Circuit Breaker** - fail fast under load
- **Caching** - Redis/Caffeine dla hot data
- **Connection Pooling** - HikariCP tuning

**Best Practices z Konferencji:**
- Performance Summit 2024: "JVM Performance Tuning"
- Kafka Summit 2024: "Kafka at Scale"
- PostgreSQL Conf 2024: "Query Optimization"

**Autorytety:**
- Martin Thompson (High-Performance Computing)
- Gil Tene (JVM Performance Expert)
- Brendan Gregg (Systems Performance)

### 📝 Ćwiczenia Praktyczne

1. **Napisz K6 load test dla EventMaster**
2. **Przeprowadź stress test (znajdź breaking point)**
3. **Zidentyfikuj slow query w PostgreSQL**
4. **Zoptymalizuj HikariCP connection pool**
5. **Monitoruj Kafka consumer lag pod obciążeniem**

### 🔗 Logiczne Połączenie z Kolejną Lekcją
Po performance testing, przechodzimy do **Production Readiness** (Lekcja 60):
- Monitoring (Prometheus, Grafana)
- Alerting (on-call, PagerDuty)
- Incident response
- Production checklist

---

## 📚 LEKCJA 60: Production Readiness & Monitoring

### 🎯 Cel Lekcji
Przygotowanie EventMaster do produkcji: monitoring (Prometheus, Grafana), distributed tracing (Jaeger), logging (ELK), alerting, incident response, security hardening.

### 📖 Zakres Wiedzy

#### Co tester musi wiedzieć o Production Readiness?

**1. Observability Pillars:**
- **Metrics** - liczniki, timery, gauges (Prometheus)
- **Logs** - structured logging (JSON), ELK stack
- **Traces** - distributed tracing (OpenTelemetry + Jaeger)

**2. EventMaster Observability:**
```
┌─────────────────────────────────────────────────┐
│  METRICS (Prometheus)                           │
├─────────────────────────────────────────────────┤
│  - http_server_requests_seconds (latency)       │
│  - kafka_consumer_lag (messaging)               │
│  - db_connection_pool_usage (database)          │
│  - jvm_memory_used_bytes (JVM)                  │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  LOGS (ELK Stack)                               │
├─────────────────────────────────────────────────┤
│  - Application logs (JSON structured)           │
│  - Access logs (Caddy)                          │
│  - Error logs (with stack traces)               │
│  - Audit logs (security events)                 │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  TRACES (Jaeger)                                │
├─────────────────────────────────────────────────┤
│  - Request span (Caddy → Backend)               │
│  - Database span (query execution)              │
│  - Kafka span (produce/consume)                 │
│  - Full trace (login → create event)            │
└─────────────────────────────────────────────────┘
```

**3. Alerting Rules:**
```yaml
groups:
  - name: eventmaster_critical
    rules:
      - alert: HighErrorRate
        expr: rate(http_server_requests_seconds_count{status=~"5.."}[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
      
      - alert: DatabaseDown
        expr: up{job="postgres"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PostgreSQL database is down"
      
      - alert: KafkaConsumerLag
        expr: kafka_consumer_lag > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer lag is high"
```

**4. Testing Concerns:**
- Metrics exposed correctly (Actuator /metrics)
- Logs structured (JSON format)
- Traces propagated (context propagation)
- Alerts triggered (test alerting rules)
- Dashboards functional (Grafana)

### 🧪 Co będziemy testować?

#### Scenariusze testowe:

**60.1 Metrics Exposure**
- Test: GET /actuator/prometheus
- Assertion: All expected metrics present
- Tool: curl + grep

**60.2 Structured Logging**
- Test: Trigger error → check log format
- Assertion: JSON structured, contains trace_id
- Tool: log parser + JSON validator

**60.3 Distributed Tracing**
- Test: Create Event E2E → check Jaeger trace
- Assertion: Trace contains all spans (HTTP, DB, Kafka)
- Tool: Jaeger Query API

**60.4 Alert Triggering**
- Test: Simulate high error rate → alert fires
- Assertion: Prometheus alert state = FIRING
- Tool: Prometheus API + curl

**60.5 Grafana Dashboard**
- Test: Load dashboard → verify data sources
- Assertion: All panels display data
- Tool: Grafana API

### 🔑 Kluczowe Koncepty

**Design Patterns:**
- **Health Check Pattern** - liveness/readiness probes
- **Circuit Breaker** - fail fast, prevent cascading failures
- **Bulkhead** - resource isolation
- **Saga Pattern** - distributed transaction monitoring

**Best Practices z Konferencji:**
- Monitorama 2024: "Observability Best Practices"
- SRECon 2024: "SLA/SLI/SLO Definitions"
- KubeCon 2024: "Production-Ready Kubernetes"

**Autorytety:**
- Charity Majors (Observability Expert, Honeycomb)
- Cindy Sridharan (Distributed Systems Expert)
- Brendan Gregg (Performance Engineering)

### 📝 Ćwiczenia Praktyczne

1. **Skonfiguruj Prometheus + Grafana dla EventMaster**
2. **Dodaj distributed tracing (OpenTelemetry)**
3. **Zaimplementuj structured logging (JSON)**
4. **Napisz alerting rules (Prometheus)**
5. **Stwórz Grafana dashboard z kluczowymi metrykami**

### 🎓 Gratulacje!

**Ukończyłeś CZĘŚĆ VI - Testowanie Praktyczne Pełnego Stosu!**

Teraz posiadasz **holistyczną wiedzę** o testowaniu każdego elementu EventMaster:
- ✅ PostgreSQL 18 (database layer)
- ✅ Keycloak (authentication/authorization)
- ✅ Spring Boot (backend)
- ✅ Kafka (messaging)
- ✅ Nuxt 3 (frontend)
- ✅ Caddy (API gateway)
- ✅ Docker (infrastructure)
- ✅ E2E, Performance, Production Readiness

**Następne kroki:**
1. Przejdź przez wszystkie 10 lekcji praktycznie
2. Implementuj testy dla EventMaster
3. Rozbuduj system o nowe features
4. Przygotuj do produkcji

---

## 📊 Podsumowanie: Architektura Testów EventMaster

### Piramida Testów (Test Pyramid)

```
                    ▲
                   ╱ ╲
                  ╱   ╲
                 ╱ E2E ╲          10% - Playwright (Lekcja 58)
                ╱───────╲
               ╱         ╲
              ╱Integration╲       30% - Testcontainers (Lekcje 51-57)
             ╱─────────────╲
            ╱               ╲
           ╱      Unit       ╲    60% - JUnit, Vitest (Części I-V)
          ╱─────────────────── ╲
         ▼                      ▼
```

### Macierz Testowania Stosu

| Warstwa | Technologia | Narzędzia Testowe | Lekcja |
|---------|-------------|-------------------|--------|
| **Database** | PostgreSQL 18 | Testcontainers, JdbcTemplate | 51 |
| **Auth** | Keycloak | Testcontainers Keycloak, MockMvc | 52 |
| **Backend** | Spring Boot 3.4 | JUnit 5, MockMvc, RestAssured | 53 |
| **Messaging** | Kafka 7.6 | EmbeddedKafka, Testcontainers | 54 |
| **Frontend** | Nuxt 3 | Vitest, Playwright, axe-core | 55 |
| **Gateway** | Caddy | curl, Gatling, Docker | 56 |
| **Infrastructure** | Docker Compose | docker-compose, health checks | 57 |
| **E2E** | Full Stack | Playwright, Testcontainers | 58 |
| **Performance** | Full Stack | Gatling, K6, JMeter | 59 |
| **Production** | Observability | Prometheus, Grafana, Jaeger | 60 |

### Design Patterns Użyte w Testach

1. **Repository Pattern** - abstrakcja dostępu do danych (PostgreSQL)
2. **Page Object Model** - abstrakcja stron UI (Playwright)
3. **Test Data Builder** - fluent creation of test data
4. **Testcontainers Pattern** - real infrastructure in tests
5. **Circuit Breaker** - resilience testing (Chaos Engineering)
6. **Retry Pattern** - eventual consistency handling
7. **Mock/Stub/Fake** - test doubles hierarchy

### Autorytety & Referencje

**Książki:**
1. "Testing Microservices with Mountebank" - Brandon Byars
2. "Continuous Delivery" - Jez Humble, David Farley
3. "Release It!" - Michael Nygard
4. "Site Reliability Engineering" - Google
5. "Accelerate" - Nicole Forsgren

**Konferencje:**
- Spring One 2024 - Spring Boot testing
- Devoxx 2024 - JVM & Kafka
- Kafka Summit 2024 - Event-driven testing
- Playwright Conf 2024 - Modern E2E
- SRECon 2024 - Production observability

**Online Resources:**
- Martin Fowler (martinfowler.com) - Testing patterns
- Test Pyramid (Mike Cohn)
- Spring Boot Testing Guide (spring.io/guides)
- Testcontainers Documentation (testcontainers.org)

---

## 🎯 Następna Iteracja: CZĘŚĆ VII - Microservices Split

Po opanowaniu testowania monolitu (EventMaster), kolejna część może obejmować:

**CZĘŚĆ VII: Testowanie Architektury Mikroserwisów**
- Lekcja 61: Decomposition - Split do Event Service + Ticket Service
- Lekcja 62: Contract Testing (Pact) między serwisami
- Lekcja 63: Service Mesh (Istio) testing
- Lekcja 64: Chaos Engineering (Chaos Monkey)
- Lekcja 65: Distributed Transactions (Saga Pattern) testing
- Lekcja 66: API Gateway (Kong/Ambassador) testing
- Lekcja 67: Service Discovery (Consul/Eureka) testing
- Lekcja 68: Configuration Management (Spring Cloud Config) testing
- Lekcja 69: Polyglot Persistence (różne bazy) testing
- Lekcja 70: Multi-Tenancy testing

---

**Status:** ✅ CZĘŚĆ VI ZAPROJEKTOWANA  
**Data utworzenia:** 2025-10-21  
**Wersja:** 1.0  
**Następny krok:** Implementacja Lekcji 51 (PostgreSQL 18 Testing)
