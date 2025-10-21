# EventMaster - Platforma Zarządzania Wydarzeniami

EventMaster to nowoczesna platforma do tworzenia i zarządzania wydarzeniami, zbudowana w architekturze **CQRS (Command Query Responsibility Segregation)** z wykorzystaniem **Event-Driven Architecture**.

## 🎯 Cel Projektu

Demonstracja zaawansowanych wzorców architektonicznych:
- ✅ CQRS - separacja komend i zapytań
- ✅ Event-Driven Architecture - komunikacja przez eventy
- ✅ Eventual Consistency - asynchroniczne przetwarzanie
- ✅ Infrastructure as Code - Keycloak realm auto-import
- ✅ Testcontainers - testy na prawdziwych technologiach

## 🚀 Technologie

### Backend
- **Java 21** + **Spring Boot 3.3.1**
- **PostgreSQL 16** (Write Model + Read Model)
- **Redpanda** (Kafka-compatible message broker)
- **Flyway** (migracje bazy danych)
- **Testcontainers** (testy integracyjne)

### Frontend
- **Nuxt 4** + **Vue 3** + **TypeScript**
- **PrimeVue** (komponenty UI)
- **Pinia** (state management)
- **@sidebase/nuxt-auth** (OAuth2/OIDC)
- **Zod** (walidacja formularzy)
- **pnpm** (package manager)

### Infrastruktura
- **Caddy** (reverse proxy)
- **Keycloak** (OAuth2/OIDC provider)
- **Docker Compose** (orkiestracja lokalna)

## 📁 Struktura Projektu

```
nuxt-learn/
├── docker/
│   ├── docker-compose.yml          # Postgres, Keycloak, Redpanda, Caddy
│   └── realm-config/
│       └── eventmaster-realm.json   # Keycloak realm (IaC)
│
├── frontend/                        # Nuxt 4 Application
│   ├── pages/
│   │   ├── index.vue               # Strona główna
│   │   └── events/
│   │       ├── index.vue           # Lista eventów
│   │       └── create.vue          # Formularz tworzenia
│   ├── nuxt.config.ts
│   └── package.json
│
├── src/                             # Spring Boot Backend
│   ├── main/java/com/eventmaster/backend/
│   │   ├── config/                 # Konfiguracja (Kafka, Security)
│   │   ├── events/
│   │   │   ├── api/                # REST Controllers
│   │   │   ├── command/            # Komendy CQRS
│   │   │   ├── domain/             # Write Model + Domain Events
│   │   │   ├── repository/         # Write Model Repository
│   │   │   ├── EventCommandHandler.java
│   │   │   └── query/
│   │   │       ├── api/            # Query Controllers
│   │   │       ├── model/          # Read Model
│   │   │       ├── repository/     # Read Model Repository
│   │   │       └── projector/      # Event Projectors
│   │   └── resources/
│   │       ├── application.properties
│   │       └── db/migration/       # Flyway migrations
│   └── test/                       # Testcontainers tests
│
├── pom.xml
├── Caddyfile
└── dokumentacja/
    ├── ARCHITEKTURA_SZCZEGOLOWA.md
    ├── ARCHITECTURE_REVIEW.md
    └── KLUCZOWE_POPRAWKI.md
```

## 🏃 Uruchomienie Projektu

### 1. Wymagania

- **Java 21** (Temurin, Corretto lub Oracle JDK)
- **Docker** + **Docker Compose**
- **pnpm** (dla frontendu)
- **Maven** (wrapper: `./mvnw`)

### 2. Uruchomienie infrastruktury

```bash
cd docker
docker-compose up -d
```

Sprawdź czy wszystkie serwisy są healthy:
```bash
docker-compose ps
```

Serwisy powinny być dostępne na:
- Caddy: http://localhost
- Keycloak: http://localhost:8180
- PostgreSQL: localhost:5432
- Redpanda: localhost:19092

### 3. Uruchomienie backendu

```bash
# Z głównego katalogu projektu
./mvnw spring-boot:run
```

Backend będzie dostępny na: http://localhost:8080

### 4. Uruchomienie frontendu

```bash
cd frontend
pnpm install
pnpm dev
```

Frontend będzie dostępny na: http://localhost:3000

### 5. Dostęp do aplikacji

Otwórz przeglądarkę: **http://localhost** (przez Caddy)

## 🧪 Testowanie

### Testy jednostkowe + integracyjne

```bash
./mvnw test
```

Testy używają **Testcontainers** do uruchomienia:
- PostgreSQL Container
- Keycloak Container (z auto-importem realm)
- Redpanda Container

### Strategia testowania

- ✅ **Zero H2** - tylko prawdziwy PostgreSQL
- ✅ **Zero EmbeddedKafka** - tylko Redpanda/Kafka
- ✅ **Awaitility** - testy asynchroniczne z timeoutami
- ✅ **@AutoConfigureTestDatabase(replace = NONE)**
- ✅ **spring.kafka.test.embedded.enabled=false**

## 🔄 Przepływ Danych (CQRS)

### Command Path (Zapis)

```
Frontend → EventCommandController → Redpanda (commands.events.create)
    ↓
EventCommandHandler → PostgreSQL (tabela: events) → Redpanda (events.lifecycle)
```

### Query Path (Odczyt)

```
Redpanda (events.lifecycle) → EventViewProjector → PostgreSQL (tabela: event_view)
    ↓
EventQueryController → Frontend
```

## 📚 Dokumentacja

- **[ARCHITEKTURA_SZCZEGOLOWA.md](ARCHITEKTURA_SZCZEGOLOWA.md)** - Pełna dokumentacja architektury (~9600 słów, ~12500 z aktualizacjami)
- **[ARCHITECTURE_REVIEW.md](ARCHITECTURE_REVIEW.md)** - Code review i rekomendacje
- **[KLUCZOWE_POPRAWKI.md](KLUCZOWE_POPRAWKI.md)** - Changelog i aktualizacje
- **[ARCHITEKTURA_AKTUALIZACJA.md](ARCHITEKTURA_AKTUALIZACJA.md)** - Precyzyjne poprawki dokumentacji

## 🔐 Bezpieczeństwo

### Keycloak Configuration

- Admin Console: http://localhost:8180
- Credentials: `admin` / `admin`
- Realm: `eventmaster` (auto-importowany z `realm-config/eventmaster-realm.json`)
- Client: `eventmaster-frontend`
- Flow: Authorization Code Flow (OAuth2/OIDC)

### JWT Validation

Backend działa jako **OAuth2 Resource Server**:
- Waliduje tokeny JWT z Keycloak
- Wymaga tokenu dla `POST /api/v1/events`
- Publiczny dostęp dla `GET /api/v1/events`

## 📊 Topiki Kafka (Redpanda)

| Topic | Typ | Producer | Consumer | Opis |
|-------|-----|----------|----------|------|
| `commands.events.create` | Command | EventCommandController | EventCommandHandler | Komendy tworzenia eventów |
| `events.lifecycle` | Domain Event | EventCommandHandler | EventViewProjector | Zdarzenia domenowe lifecycle |

## 🎨 Frontend - Funkcjonalności

### Zaimplementowane
- ✅ Logowanie przez Keycloak (OAuth2)
- ✅ Formularz tworzenia eventu (`/events/create`)
- ✅ Walidacja formularzy (Zod + vee-validate)
- ✅ PrimeVue UI components
- ✅ Middleware autoryzacji

### TODO
- ⏳ Wyświetlanie listy eventów (`/events`)
- ⏳ Szczegóły eventu
- ⏳ Edycja eventu
- ⏳ Usuwanie eventu

## 🛠️ Narzędzia Development

### Caddy (Reverse Proxy)

Routing:
- `/api/v1/*` → Spring Boot (localhost:8080)
- `/api/auth/*` → Nuxt Server (localhost:3000)
- `/*` → Nuxt Frontend (localhost:3000)

### Flyway Migrations

Lokalizacja: `src/main/resources/db/migration/`

```bash
# Stosuj migracje
./mvnw flyway:migrate

# Status migracji
./mvnw flyway:info
```

### Monitorowanie Redpanda

```bash
# Dostęp do kontenera
docker exec -it docker-redpanda-1 /bin/bash

# Lista topików
rpk topic list

# Szczegóły topiku
rpk topic describe commands.events.create

# Konsumuj wiadomości
rpk topic consume commands.events.create --offset start
```

## 🐛 Troubleshooting

### Backend nie może połączyć się z Postgres
```bash
# Sprawdź czy Postgres działa
docker-compose ps postgres

# Sprawdź logi
docker-compose logs postgres

# Restart
docker-compose restart postgres
```

### Keycloak nie importuje realm
```bash
# Sprawdź logi
docker-compose logs keycloak

# Verify volume mount
docker-compose exec keycloak ls -la /opt/keycloak/data/import/
```

### Redpanda nie startuje
```bash
# Sprawdź logi
docker-compose logs redpanda

# Health check
docker-compose exec redpanda rpk cluster health
```

## 🤝 Kontrybucje

Projekt edukacyjny demonstrujący CQRS i Event-Driven Architecture.

## 📝 Licencja

MIT License - możesz swobodnie używać w celach edukacyjnych i komercyjnych.

---

**Autor:** EventMaster Team  
**Data utworzenia:** 2025-10-19  
**Status:** Development (MVP - Command Path + Query Path zaimplementowane)

---

## 🎓 WARSZTATY PRAKTYCZNE (Nowe!)

**Format:** Architekt implementuje → Tester (TY!) piszesz testy  
**Dokumentacja:** 

📚 **[WARSZTATY PRAKTYCZNE - INDEKS](./learning-path/WARSZTATY_PRAKTYCZNE_INDEX.md)**

### Rozpocznij tutaj:

👉 **[LEKCJA 51 - WARSZTAT: PostgreSQL 18 Testing (Dzień 1)](./learning-path/CZESC_VI_LEKCJA_51_WARSZTAT.md)**

**Format warsztatu:**
1. **ARCHITEKT** tworzy kod (Backend/Frontend) - już zaimplementowane
2. **TESTER (TY!)** piszesz testy - instrukcje krok po kroku
3. **URUCHAMIASZ** testy (`./mvnw test`)
4. **COMMIT** & przechodź do kolejnego dnia

**Czas:** 5 dni × 3-4h = 1 tydzień na lekcję  
**Cel:** Hands-on experience - od teorii do praktyki

