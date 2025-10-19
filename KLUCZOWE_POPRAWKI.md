# Kluczowe Poprawki w Projekcie EventMaster

## Status po analizie (2025-10-19)

### ✅ Co zostało poprawione:

1. **Dodano Redpanda do docker-compose.yml**
   - Usunięto CockroachDB (nie był używany w obecnej wersji)
   - Dodano pełną konfigurację Redpanda z health checkami
   - Port Kafka: 19092 (external), 9092 (internal)

2. **Zaktualizowano dokumentację architektury**
   - Poprawiono informacje o używaniu jednego DataSource (PostgreSQL)
   - Dodano szczegóły o Redpanda zamiast "ogólnej Kafki"
   - Wyjaśniono, że Write i Read Model są w tej samej bazie (różne tabele)

### 📋 Aktualny stan technologii:

**Backend:**
- Spring Boot 3.3.1 (nie 6.2+ - to był błąd w opisie)
- Java 21
- **Jeden DataSource** - PostgreSQL na localhost:5432
- Write Model: tabela `events` (zarządzana Flyway)
- Read Model: tabela `event_view` (zarządzana Hibernate auto-update)
- Kafka/Redpanda na localhost:19092

**Frontend:**
- Nuxt 4.0
- TypeScript (strict mode)
- pnpm jako package manager
- PrimeVue dla UI
- Pinia dla state management
- @sidebase/nuxt-auth dla OAuth2/OIDC

**Infrastruktura:**
- Caddy (reverse proxy) - port 80
- Keycloak - port 8180 (z auto-importem realm)
- Redpanda - port 19092 (Kafka compatible)
- PostgreSQL - port 5432

### 🔄 Topiki Kafka:

1. **commands.events.create** - Komendy tworzenia eventów
2. **events.lifecycle** - Domain Events (EventCreatedEvent)

### 🧪 Testowanie:

**Testcontainers:**
- PostgreSQLContainer
- KeycloakContainer (z importem realm z `test-realm-config.json`)
- RedpandaContainer

**Strategia:**
- Zero H2, Zero EmbeddedKafka
- Awaitility dla testów asynchronicznych
- @AutoConfigureTestDatabase(replace = NONE)
- spring.kafka.test.embedded.enabled=false

### 📁 Struktura obecna:

```
nuxt-learn/
├── docker/
│   ├── docker-compose.yml
│   └── realm-config/
│       └── eventmaster-realm.json
├── frontend/
│   ├── pages/
│   ├── nuxt.config.ts
│   └── package.json (pnpm)
├── src/
│   ├── main/
│   │   ├── java/com/eventmaster/backend/
│   │   └── resources/
│   └── test/
│       └── java/com/eventmaster/backend/
├── pom.xml
└── Caddyfile
```

### ⚠️ Uwagi:

1. **CockroachDB był w docker-compose, ale NIE był używany**
   - Konfiguracja wskazywała na niego (spring.datasource.read)
   - Faktycznie oba DataSources używają PostgreSQL w testach
   - Usunięto CockroachDB, pozostawiono tylko PostgreSQL

2. **Write i Read DataSource konfiguracja istnieje**, ale:
   - Oba używają tego samego PostgreSQL
   - Write: tabela `events`
   - Read: tabela `event_view`
   - To uproszczenie na etapie developmentu

3. **Flyway zarządza tylko Write Model**
   - Read Model używa `hibernate.hbm2ddl.auto=update`
   - To świadome uproszczenie

### 🎯 Następne kroki (sugerowane):

1. ✅ Dodać Redpanda do docker-compose - **ZROBIONE**
2. Dodać health checks do wszystkich serwisów w docker-compose
3. Podłączyć EventQueryController do EventViewRepository
4. Dodać frontend endpoint do pobierania listy eventów
5. Rozważyć: czy przenieść src/ do backend/src/ (reorganizacja struktury)

### 📝 Dokumenty:

- **ARCHITEKTURA_SZCZEGOLOWA.md** - Pełna dokumentacja architektury (~9600 słów)
- **ARCHITECTURE_REVIEW.md** - Przegląd architektury CQRS (oryginalny)
- **KLUCZOWE_POPRAWKI.md** - Ten dokument (podsumowanie zmian)

---

**Data aktualizacji:** 2025-10-19
**Autor poprawek:** System review + user feedback
