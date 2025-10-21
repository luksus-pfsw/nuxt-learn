# 🎓 WARSZTATY PRAKTYCZNE - EventMaster Testing

**Format:** Architekt implementuje kod → Tester (TY!) piszesz testy  
**Czas:** 10 tygodni (10 lekcji × 5 dni roboczych)  
**Cel:** Od teorii do praktyki - hands-on testing experience

---

## 📚 JAK KORZYSTAĆ Z WARSZTATÓW?

### Format Każdej Lekcji:

```
LEKCJA XX: [Temat] - WARSZTAT PRAKTYCZNY
│
├── DZIEŃ 1: [Podtemat A]
│   ├── Krok 1: ARCHITEKT implementuje kod (Backend/Frontend)
│   ├── Krok 2: TESTER pisze testy (TY!)
│   ├── Krok 3: Checkpoint - uruchom testy
│   └── Zadania domowe (opcjonalnie)
│
├── DZIEŃ 2: [Podtemat B]
│   ├── Krok 1: ARCHITEKT...
│   ├── Krok 2: TESTER (TY!)...
│   └── ...
│
├── DZIEŃ 3-5: ...
└── ✅ CHECKPOINT TYGODNIA
```

### Twoja Rola (TESTER):

1. **Czytasz kod** zaimplementowany przez Architekta
2. **Piszesz testy** zgodnie z instrukcjami
3. **Uruchamiasz testy** (`./mvnw test`, `npm test`)
4. **Commit & Push** (daily progress)
5. **Code Review** (z Architektem - opcjonalnie)

---

## 📋 INDEKS WARSZTATÓW (Lekcje 51-60)

### ✅ LEKCJA 51: PostgreSQL 18 Features & Testing

📄 **[Warsztat Lekcja 51 - Dzień 1: MERGE Statement](./CZESC_VI_LEKCJA_51_WARSZTAT.md)**

**Tygodniowy plan:**
- **Dzień 1:** Setup Testcontainers + MERGE Statement + Idempotency Tests
- **Dzień 2:** Optimistic Locking (@Version) + Concurrent Modification Tests
- **Dzień 3:** Query Performance (EXPLAIN ANALYZE) + Index Optimization Tests
- **Dzień 4:** Flyway Migrations + Rollback Tests
- **Dzień 5:** HikariCP Connection Pool + Exhaustion Tests

**Status:** ✅ DZIEŃ 1 GOTOWY (dokumentacja utworzona)  
**Następne:** Dzień 2-5 (będą tworzone na żądanie)

---

### ⏳ LEKCJA 52: Keycloak OAuth2 Flow & Security Testing

📄 **Warsztat Lekcja 52** (zostanie utworzony po ukończeniu Lekcji 51)

**Tygodniowy plan:**
- **Dzień 1:** Setup Testcontainers Keycloak + Realm Configuration
- **Dzień 2:** Authorization Code Flow E2E Test (Playwright)
- **Dzień 3:** JWT Validation Tests (MockMvc + RestAssured)
- **Dzień 4:** RBAC Tests (@PreAuthorize, @PostAuthorize)
- **Dzień 5:** Token Refresh + Multi-Client Scenarios

**Status:** ⏳ Oczekuje na ukończenie Lekcji 51

---

### ⏳ LEKCJA 53: Spring Boot Security Integration Testing

**Tygodniowy plan:**
- **Dzień 1:** SecurityFilterChain Configuration + Basic Auth Tests
- **Dzień 2:** JWT Authentication Tests (@WithMockJwt)
- **Dzień 3:** Method Security Tests (@PreAuthorize)
- **Dzień 4:** CORS Configuration Tests
- **Dzień 5:** Actuator Security Tests

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 54: Kafka Advanced Scenarios Testing

**Tygodniowy plan:**
- **Dzień 1:** Partitioning Strategies + Key-Based Routing Tests
- **Dzień 2:** Consumer Group Rebalancing Tests
- **Dzień 3:** Exactly-Once Semantics (Transactional Outbox) Tests
- **Dzień 4:** Schema Evolution (Avro) + Backward Compatibility Tests
- **Dzień 5:** Consumer Lag Monitoring + Performance Tests

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 55: Nuxt 3 SSR & Frontend Integration Testing

**Tygodniowy plan:**
- **Dzień 1:** SSR Rendering Tests (Playwright + page.content())
- **Dzień 2:** Auth Middleware Tests (redirect scenarios)
- **Dzień 3:** API Routes Tests (Nitro server)
- **Dzień 4:** Component Tests (Vitest + @vue/test-utils)
- **Dzień 5:** Accessibility Tests (axe-core)

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 56: Caddy Reverse Proxy & Routing Testing

**Tygodniowy plan (skrócony - 2.5 dnia):**
- **Dzień 1:** Path-Based Routing Tests (curl + assertions)
- **Dzień 2:** Load Balancing Tests (2+ backend instances)
- **Dzień 3 (połowa):** Health Check Failover Tests

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 57: Docker & Container Testing

**Tygodniowy plan (skrócony - 2.5 dnia):**
- **Dzień 1:** Docker Compose Orchestration Tests (depends_on, health checks)
- **Dzień 2:** Resource Limits Tests (OOMKilled scenarios)
- **Dzień 3 (połowa):** Volume Persistence Tests

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 58: End-to-End Flow Testing (Full Stack)

**Tygodniowy plan:**
- **Dzień 1:** Complete User Journey Test (Login → Create → View → Logout)
- **Dzień 2:** Eventual Consistency E2E Test (with Awaitility)
- **Dzień 3:** Multi-User Scenario Test (2 browser contexts)
- **Dzień 4:** Error Handling E2E Test
- **Dzień 5:** Page Object Model Refactoring

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 59: Performance & Load Testing (Full Stack)

**Tygodniowy plan:**
- **Dzień 1:** K6 Load Test (Read Endpoints) - 1000 req/s
- **Dzień 2:** K6 Load Test (Write Endpoints) - 100 req/s
- **Dzień 3:** Gatling Stress Test (Breaking Point)
- **Dzień 4:** PostgreSQL Query Performance (EXPLAIN ANALYZE)
- **Dzień 5:** Kafka Consumer Lag Monitoring

**Status:** ⏳ Oczekuje

---

### ⏳ LEKCJA 60: Production Readiness & Monitoring

**Tygodniowy plan:**
- **Dzień 1:** Prometheus Metrics Exposure Tests
- **Dzień 2:** Structured Logging Tests (JSON format)
- **Dzień 3:** Distributed Tracing Tests (Jaeger)
- **Dzień 4:** Alerting Rules Tests (Prometheus)
- **Dzień 5:** Grafana Dashboard Tests

**Status:** ⏳ Oczekuje

---

## 🚀 QUICK START

### Krok 1: Przygotuj Środowisko (jednorazowo)

```bash
# Sklonuj repo (jeśli jeszcze nie masz)
cd /home/tester/projects/nuxt-learn

# Uruchom infrastrukturę (Docker Compose)
cd docker
docker-compose up -d

# Sprawdź czy działa
docker-compose ps
# Expected: postgres, keycloak, kafka, zookeeper, schema-registry - all healthy
```

### Krok 2: Rozpocznij Lekcję 51 - Dzień 1

```bash
# Otwórz dokument warsztatu
cat learning-path/CZESC_VI_LEKCJA_51_WARSZTAT.md

# LUB w przeglądarce:
# https://github.com/[repo]/blob/main/learning-path/CZESC_VI_LEKCJA_51_WARSZTAT.md
```

### Krok 3: Implementuj Kod (ARCHITEKT)

**Backend:**
```bash
cd backend

# Architekt tworzy pliki (zgodnie z warsztatem):
# - BaseIntegrationTest.java
# - Event.java
# - EventRepository.java
# - V1__Create_Events_Table.sql
```

### Krok 4: Pisz Testy (TESTER - TY!)

**Backend:**
```bash
# Ty tworzysz:
# - MergeStatementTest.java

# Uruchom testy
./mvnw test -Dtest=MergeStatementTest

# Expected:
# [INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

### Krok 5: Commit & Następny Dzień

```bash
git add .
git commit -m "Lekcja 51 Dzień 1: MERGE statement tests"
git push

# Przejdź do Dzień 2
# (Dokument zostanie utworzony na żądanie)
```

---

## 📊 TRACKING POSTĘPÓW

### Checklist Tygodniowa (dla każdej lekcji):

**Lekcja 51: PostgreSQL 18**
- [ ] Dzień 1: MERGE Statement ✅
- [ ] Dzień 2: Optimistic Locking ⏳
- [ ] Dzień 3: Query Performance ⏳
- [ ] Dzień 4: Flyway Migrations ⏳
- [ ] Dzień 5: Connection Pool ⏳

**Lekcja 52: Keycloak**
- [ ] Dzień 1-5: ⏳

... (itd. dla wszystkich 10 lekcji)

---

## 🎯 METRYKI SUKCESU

### Po Każdym Tygodniu (Lekcji):

| Metryka | Target | Twój Wynik |
|---------|--------|------------|
| **Testy napisane** | 15-20 | ___ |
| **Test Coverage** | 80%+ | ___% |
| **Testy passing** | 100% | ___% |
| **Czas wykonania** | < 30s | ___s |
| **Zrozumienie tematu** | 8/10 | ___/10 |

### Po Całej CZĘŚCI VI (10 tygodni):

| Metryka | Target |
|---------|--------|
| **Lekcje ukończone** | 10/10 |
| **Testy napisane** | 150-200 |
| **Test Coverage (Backend)** | 80%+ |
| **Test Coverage (Frontend)** | 70%+ |
| **Poziom umiejętności** | Test Architect |

---

## 🆘 POMOC & WSPARCIE

### Masz Problem?

1. **Najpierw:** Sprawdź FAQ w dokumencie warsztatu
2. **Potem:** Sprawdź `CZESC_VI_TESTOWANIE_PRAKTYCZNE_STOSU.md` (teoria)
3. **W razie błędu:** Sprawdź logi (`./mvnw test -X`)
4. **Stack Overflow:** Wyszukaj błąd
5. **GitHub Issues:** Zadaj pytanie (tag: help-wanted)

### Code Review (Opcjonalnie)

Jeśli chcesz feedback od Architekta:
1. Commit testów do brancha (`feature/lekcja-51-dzien-1`)
2. Stwórz Pull Request
3. Poproś o review (tag: @architect)

---

## 📚 DOKUMENTY WSPIERAJĄCE

### Teoria (przeczytaj przed warsztatem):
- **[CZESC_VI_TESTOWANIE_PRAKTYCZNE_STOSU.md](./CZESC_VI_TESTOWANIE_PRAKTYCZNE_STOSU.md)** - Pełny przewodnik teoretyczny
- **[CZESC_VI_QUICK_REFERENCE.md](./CZESC_VI_QUICK_REFERENCE.md)** - Krótkie opisy każdej lekcji

### Strategi (dla managera/architekta):
- **[CZESC_VI_STRATEGIC_ANALYSIS.md](./ANALIZA_STRATEGICZNA_CZESC_VI.md)** - Analiza strategiczna
- **[CZESC_VI_SUMMARY.md](../CZESC_VI_SUMMARY.md)** - Executive summary

### Wizualizacje:
- **[CZESC_VI_DIAGRAMY.txt](./CZESC_VI_DIAGRAM.txt)** - ASCII diagramy

---

## 🎓 NASTĘPNY KROK

**Jeśli jesteś gotowy:**

👉 **[ROZPOCZNIJ LEKCJĘ 51 - DZIEŃ 1: PostgreSQL 18 MERGE Statement](./CZESC_VI_LEKCJA_51_WARSZTAT.md)**

**Czas:** ~3-4 godziny  
**Format:** Czytasz kod → Piszesz testy → Uruchamiasz → Commit  
**Efekt:** Opanujesz MERGE statement w PostgreSQL 18 + Testcontainers

---

## 🎉 MOTYWACJA

**Pamiętaj:**
- ✅ **Każdy test** to kolejny krok w kierunku Test Architect
- ✅ **Każdy błąd** to lekcja (bugs are features!)
- ✅ **Każdy commit** to postęp (small wins!)
- ✅ **Każdy tydzień** to nowa umiejętność

**Po 10 tygodniach będziesz:**
- ✅ Ekspertem od testowania systemów rozproszonych
- ✅ Opanowałeś cały stos (PostgreSQL → Kubernetes)
- ✅ Gotowy do certyfikatu (ISTQB TAE, CKAD)
- ✅ **Test Architect / SDET Senior** 🎖️

---

**Status:** ✅ INDEKS WARSZTATÓW GOTOWY  
**Rozpocznij:** Lekcja 51 - Dzień 1  
**Format:** Architekt → Kod | Tester (TY!) → Testy

🎓 **Let's code & test! Powodzenia! 🚀**

---

**Utworzono:** 2025-10-21  
**Wersja:** 1.0  
**Autor:** EventMaster Architecture Team
