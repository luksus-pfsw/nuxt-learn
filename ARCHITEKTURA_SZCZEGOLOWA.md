# EventMaster - Szczegółowa Architektura Systemu CQRS

**Dokument architektoniczny dla 15-latka (z terminologią techniczną)**

**Wersja:** 1.0  
**Data:** 2025-10-19  
**Autorzy:** Zespół EventMaster

---

## Spis treści

1. [Wprowadzenie - Co to jest EventMaster?](#1-wprowadzenie)
2. [Podstawy CQRS - Podział na Komendy i Zapytania](#2-podstawy-cqrs)
3. [Architektura wysokopoziomowa](#3-architektura-wysokopoziomowa)
4. [Kontenery i komponenty systemu](#4-kontenery-i-komponenty-systemu)
5. [Backend Java - szczegółowa analiza](#5-backend-java)
6. [Frontend Nuxt.js - szczegółowa analiza](#6-frontend-nuxtjs)
7. [Przepływ danych - scenariusze krok po kroku](#7-przepływ-danych)
8. [Bazy danych i separacja modeli](#8-bazy-danych-i-separacja-modeli)
9. [Kafka - autobus komunikacyjny](#9-kafka-autobus-komunikacyjny)
10. [Bezpieczeństwo i autoryzacja](#10-bezpieczeństwo-i-autoryzacja)
11. [Infrastruktura i deployment](#11-infrastruktura-i-deployment)
12. [Słowniczek pojęć](#12-słowniczek-pojęć)

---

## 1. Wprowadzenie

### 1.1 Co to jest EventMaster?

EventMaster to aplikacja do tworzenia i przeglądania wydarzeń (eventów), podobnie jak Facebook Events czy Meetup.com. Użytkownicy mogą:
- **Tworzyć** nowe wydarzenia (koncerty, konferencje, spotkania)
- **Przeglądać** listę wszystkich nadchodzących wydarzeń

### 1.2 Czym EventMaster jest wyjątkowy?

W przeciwieństwie do standardowych aplikacji, EventMaster wykorzystuje **architekturę CQRS** (Command Query Responsibility Segregation), co oznacza:
- **Osobny tor** dla tworzenia danych (komendy)
- **Osobny tor** dla odczytywania danych (zapytania)
- **Dwie osobne tabele** w bazie danych optymalizowane pod różne cele

To jak gdybyś miał dwa różne notesy: jeden do pisania notatek, drugi do ich czytania, każdy z inną strukturą, żeby było najwydajniej.

**Uwaga o implementacji:** W obecnej wersji EventMaster używamy jednej bazy PostgreSQL z dwiema oddzielnymi tabelami (`events` dla zapisu, `event_view` dla odczytu). To uproszczenie na etapie MVP - w produkcji można rozdzielić na osobne bazy danych.

### 1.3 Technologie

```
Backend:  Java 21 + Spring Boot 3.3.1
Frontend: Nuxt 4 + Vue 3 + TypeScript
Message Broker: Redpanda (Kafka-compatible)
Database: PostgreSQL 16
  - Write Model: tabela "events"
  - Read Model: tabela "event_view"
Auth: Keycloak (OAuth2/OIDC)
Proxy: Caddy
Testing: Testcontainers (PostgreSQL, Keycloak, Redpanda)
```

---

## 2. Podstawy CQRS

### 2.1 Co to jest CQRS?

**CQRS** = Command Query Responsibility Segregation (Separacja odpowiedzialności komend i zapytań)

Wyobraź sobie bibliotekę:
- **Komenda (Write)**: To jak wypożyczenie książki - musisz wypełnić formularz, bibliotekarz musi sprawdzić dostępność, zapisać w systemie, itp. To **skomplikowana operacja**.
- **Zapytanie (Query/Read)**: To jak sprawdzenie katalogu książek - po prostu szybkie przejrzenie listy. To **prosta operacja**.

W tradycyjnej aplikacji obie te operacje korzystają z tej samej bazy danych i tego samego modelu. W CQRS **rozdzielamy je całkowicie**.

### 2.2 Dlaczego to robimy?

```mermaid
graph LR
    A[Użytkownik tworzy event] --> B[Komenda]
    C[Użytkownik przegląda eventy] --> D[Zapytanie]
    
    B --> E[Tabela: events<br/>PostgreSQL<br/>Write Model<br/>Zarządzana przez Flyway]
    D --> F[Tabela: event_view<br/>PostgreSQL<br/>Read Model<br/>Zarządzana przez Hibernate]
    
    style B fill:#ff6b6b
    style D fill:#51cf66
    style E fill:#ff6b6b
    style F fill:#51cf66
```

**Zalety:**
1. **Wydajność**: Baza do odczytu może mieć prostszą strukturę (bez JOIN-ów), więc zapytania są szybkie
2. **Skalowalność**: Możemy mieć wiele kopii bazy odczytu (read replicas)
3. **Niezależność**: Zmiana w strukturze zapisu nie wpływa na odczyt
4. **Optymalizacja**: Każda baza jest zoptymalizowana pod swoje zadanie

**Wady:**
1. **Eventual Consistency**: Dane nie są dostępne natychmiast po zapisie (trzeba poczekać kilka milisekund)
2. **Złożoność**: Więcej kodu, więcej baz danych, więcej rzeczy do zarządzania

---

## 3. Architektura wysokopoziomowa

### 3.1 Diagram kontekstu systemu

```mermaid
C4Context
    title Diagram Kontekstu Systemu EventMaster

    Person(user, "Użytkownik", "Osoba korzystająca z aplikacji")
    
    System_Boundary(eventmaster, "EventMaster System") {
        System(frontend, "Frontend Nuxt.js", "Interfejs użytkownika")
        System(backend, "Backend Spring Boot", "Logika biznesowa")
        System(redpanda, "Redpanda", "Message Broker (Kafka-compatible)")
    }
    
    System_Ext(keycloak, "Keycloak", "Serwer autoryzacji OAuth2")
    SystemDb(postgres, "PostgreSQL", "Baza danych (Write + Read Model)")
    
    Rel(user, frontend, "Używa", "HTTPS")
    Rel(frontend, backend, "Wywołuje API", "HTTP/REST")
    Rel(frontend, keycloak, "Loguje się", "OAuth2/OIDC")
    
    Rel(backend, redpanda, "Publikuje/Konsumuje", "Kafka Protocol")
    Rel(backend, postgres, "Zapisuje i Odczytuje", "JDBC")
    Rel(backend, keycloak, "Waliduje token", "JWT")
    
    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="2")
```

**Uwaga:** PostgreSQL przechowuje dwie oddzielne tabele:
- `events` - Write Model (zarządzana przez Flyway)
- `event_view` - Read Model (zarządzana przez Hibernate)

### 3.2 Główne komponenty

| Komponent | Rola | Technologia | Port |
|-----------|------|-------------|------|
| **Frontend** | Interfejs użytkownika, formularze, wyświetlanie listy | Nuxt 4, Vue 3, TypeScript | 3000 |
| **Backend** | Logika biznesowa, walidacja, przetwarzanie komend | Spring Boot 3, Java 21 | 8080 |
| **Redpanda** | Kolejka wiadomości, event bus (Kafka-compatible) | Redpanda | 19092 (ext), 9092 (int) |
| **PostgreSQL** | Baza danych (Write + Read Model w osobnych tabelach) | PostgreSQL 16 | 5432 |
| **Keycloak** | Serwer autoryzacji, logowanie użytkowników | Keycloak | 8180 |
| **Caddy** | Reverse proxy, routing HTTP | Caddy | 80/443 |

---

## 4. Kontenery i komponenty systemu

### 4.1 Architektura kontenerowa

```mermaid
graph TB
    subgraph "Docker Compose Environment"
        subgraph "Warstwa prezentacji"
            CADDY[Caddy Proxy<br/>Port 80]
            NUXT[Nuxt.js Frontend<br/>Port 3000]
        end
        
        subgraph "Warstwa logiki"
            SPRING[Spring Boot Backend<br/>Port 8080]
        end
        
        subgraph "Warstwa komunikacji"
            REDPANDA[Redpanda<br/>Port 19092 external<br/>Port 9092 internal]
        end
        
        subgraph "Warstwa danych"
            PG[(PostgreSQL<br/>Tabela: events Write<br/>Tabela: event_view Read<br/>Port 5432)]
        end
        
        subgraph "Warstwa bezpieczeństwa"
            KC[Keycloak<br/>Port 8180]
        end
    end
    
    USER((Użytkownik<br/>Przeglądarka))
    
    USER -->|HTTP| CADDY
    CADDY -->|/api/v1/*| SPRING
    CADDY -->|/*| NUXT
    CADDY -->|/api/auth/*| NUXT
    
    NUXT -.->|OAuth2 Login| KC
    SPRING -.->|JWT Validation| KC
    
    SPRING -->|Publish/Subscribe| REDPANDA
    SPRING -->|Write to events table| PG
    SPRING -->|Read from event_view table| PG
    
    style USER fill:#e1f5ff
    style CADDY fill:#fff4e6
    style NUXT fill:#d0ebff
    style SPRING fill:#ffd8a8
    style REDPANDA fill:#ffc9c9
    style PG fill:#d3f9d8
    style KC fill:#e7f5ff
```

**Wyjaśnienie zmian względem typowego CQRS:**
- Zamiast dwóch osobnych baz danych (PostgreSQL + CockroachDB), używamy jednej bazy PostgreSQL z dwiema tabelami
- To uproszczenie na etapie MVP pozwala łatwiej testować i rozwijać system
- Write Model: tabela `events` (zarządzana przez Flyway migrations)
- Read Model: tabela `event_view` (zarządzana przez Hibernate ddl-auto=update)
- Redpanda to lżejsza alternatywa dla Apache Kafka (Kafka-compatible API)

### 4.2 Opis każdego kontenera

#### 4.2.1 Caddy Proxy

**Co to jest?**  
Caddy to reverse proxy - działa jak recepcjonista w hotelu. Gdy przychodzi żądanie HTTP, Caddy sprawdza adres URL i przekierowuje je do odpowiedniego serwisu.

**Co przechowuje?**
- Konfiguracja routingu (Caddyfile)
- Certyfikaty SSL (automatycznie generowane)

**Jak komunikuje się z innymi?**
```
Użytkownik → Caddy → Decyzja routingu:
  ├─ /api/v1/*     → Spring Boot (port 8080)
  ├─ /api/auth/*   → Nuxt Server (port 3000)
  └─ /*            → Nuxt Frontend (port 3000)
```

**Przykład:**
- Żądanie: `GET http://localhost/api/v1/events` → Caddy przekierowuje do `http://localhost:8080/api/v1/events`
- Żądanie: `GET http://localhost/` → Caddy przekierowuje do `http://localhost:3000/`

---

#### 4.2.2 Nuxt.js Frontend

**Co to jest?**  
Frontend to interfejs użytkownika - wszystko co widzisz w przeglądarce. Nuxt.js to framework do budowania aplikacji Vue.js z SSR (Server-Side Rendering).

**Główne komponenty:**
1. **Strony (Pages)**:
   - `/pages/index.vue` - Strona główna
   - `/pages/events/index.vue` - Lista wydarzeń
   - `/pages/events/create.vue` - Formularz tworzenia eventu

2. **Middleware**:
   - `auth` - Sprawdza czy użytkownik jest zalogowany

3. **Server API**:
   - `/server/api/auth/[...].ts` - Obsługa callbacku OAuth2

**Co przechowuje?**
- Tokeny OAuth2 (w session cookies)
- Stan aplikacji (reactive state w Vue)
- Komponenty UI

**Jak komunikuje się?**
```mermaid
sequenceDiagram
    participant Browser
    participant Nuxt
    participant SpringBoot
    participant Keycloak
    
    Browser->>Nuxt: GET /events/create
    Nuxt->>Nuxt: Middleware auth sprawdza token
    alt Brak tokenu
        Nuxt->>Keycloak: Przekieruj do logowania
        Keycloak->>Nuxt: Callback z kodem
        Nuxt->>Keycloak: Wymień kod na token
        Keycloak->>Nuxt: Zwróć JWT token
    end
    Nuxt->>Browser: Renderuj stronę
    Browser->>Nuxt: Submit formularz
    Nuxt->>SpringBoot: POST /api/v1/events (z tokenem JWT)
    SpringBoot->>Nuxt: 202 Accepted
    Nuxt->>Browser: Przekieruj na /events
```

---

#### 4.2.3 Spring Boot Backend

**Co to jest?**  
Backend to serce aplikacji - tutaj dzieje się cała logika biznesowa, walidacja, przetwarzanie komend i zdarzeń.

**Struktura pakietów:**

```
com.eventmaster.backend/
├── config/
│   ├── KafkaConfig.java           # Konfiguracja Kafki (error handling, DLQ)
│   └── SecurityConfig.java        # Konfiguracja OAuth2 Resource Server
│
├── configs/persistence/
│   ├── WriteDataSourceConfig.java # Konfiguracja DataSource dla Write Model
│   └── ReadDataSourceConfig.java  # Konfiguracja DataSource dla Read Model
│
├── events/
│   ├── api/
│   │   ├── EventCommandController.java  # REST endpoint dla komend POST
│   │   └── CreateEventRequest.java      # DTO requestu
│   │
│   ├── command/
│   │   └── CreateEventCommand.java      # Komenda (immutable record)
│   │
│   ├── domain/
│   │   ├── Event.java                   # Agregat (Write Model Entity)
│   │   └── EventCreatedEvent.java       # Zdarzenie domenowe
│   │
│   ├── repository/
│   │   └── EventRepository.java         # JPA Repository (Write Model)
│   │
│   ├── EventCommandHandler.java         # Kafka Listener, zapisuje do Write DB
│   │
│   └── query/
│       ├── api/
│       │   └── EventQueryController.java    # REST endpoint dla GET
│       ├── dto/
│       │   └── EventViewDTO.java            # DTO response
│       ├── model/
│       │   └── EventView.java               # Read Model Entity
│       ├── repository/
│       │   └── EventViewRepository.java     # JPA Repository (Read Model)
│       └── projector/
│           └── EventViewProjector.java      # Kafka Listener, zapisuje do Read DB
│
└── web/
    ├── PingController.java         # Health check
    └── UserInfoController.java     # Info o zalogowanym użytkowniku
```

**Co przechowuje?**
- Nic! Backend jest stateless (bezstanowy). Wszystkie dane w bazach danych.

**Jak komunikuje się?**
- **HTTP REST API** - Przyjmuje żądania od frontendu
- **Redpanda Producer** - Wysyła komendy i zdarzenia (Kafka-compatible API)
- **Redpanda Consumer** - Słucha komend i zdarzeń
- **JDBC** - Łączy się z PostgreSQL (dwie tabele: events i event_view)

---


#### 4.2.4 Redpanda (Message Broker)

**Co to jest?**  
Redpanda to message broker - "autobus wiadomości", przez który płyną wszystkie komendy i zdarzenia w aplikacji. To jak poczta w firmie - każdy może wysłać list (message) na określony adres (topic), a zainteresowani mogą go odebrać.

**Redpanda vs Apache Kafka:**
- Redpanda jest **kompatybilny z API Kafki** - ten sam protokół, te same biblioteki klienckie
- **Lżejszy** - nie wymaga Zookeeper, mniej zasobów
- **Szybszy start** - idealny do developmentu i testów
- **Łatwiejszy setup** - jedno narzędzie CLI (`rpk`)

**Główne koncepcje:**

1. **Topic** (Temat):
   - To jak skrzynka pocztowa z nazwą
   - W EventMaster mamy 2 topici:
     - `commands.events.create` - Komendy tworzenia eventów
     - `events.lifecycle` - Zdarzenia domenowe (zmieniona nazwa z domain.events.lifecycle)

2. **Producer** (Producent):
   - Komponent który wysyła wiadomości do topicu
   - W EventMaster: `EventCommandController` i `EventCommandHandler`

3. **Consumer** (Konsument):
   - Komponent który odbiera wiadomości z topicu
   - W EventMaster: `EventCommandHandler` i `EventViewProjector`

4. **Consumer Group**:
   - Grupa konsumentów pracujących razem
   - Każda wiadomość jest przetwarzana przez **jednego** konsumenta z grupy
   - Grupy w EventMaster:
     - `event-command-handler` - Przetwarza komendy
     - `eventmaster-projectors-crdb` - Projektuje do Read Model (nazwa historyczna)

**Co przechowuje?**
- Wszystkie wiadomości przez określony czas (domyślnie 7 dni w Redpanda)
- Offset (pozycja) każdego consumera - "który message ostatnio przeczytał"

**Dlaczego używamy Redpanda?**
1. **Asynchroniczność**: Nie musimy czekać na przetworzenie - zwracamy 202 Accepted od razu
2. **Niezawodność**: Jeśli backend padnie, wiadomości czekają w Redpanda
3. **Retry**: Automatyczne ponowne próby przy błędach
4. **Audytowalność**: Wszystkie komendy są zapisane
5. **Prostota**: Łatwiejszy w setup niż klasyczna Kafka (brak Zookeeper)

**Przepływ przez Redpanda:**
```mermaid
graph LR
    A[EventCommandController] -->|Publish| B[Topic: commands.events.create]
    B -->|Subscribe| C[EventCommandHandler]
    C -->|Publish| D[Topic: events.lifecycle]
    D -->|Subscribe| E[EventViewProjector]
    
    style A fill:#ffd8a8
    style C fill:#ffd8a8
    style E fill:#ffd8a8
    style B fill:#ffc9c9
    style D fill:#ffc9c9
```

---

#### 4.2.5 PostgreSQL - Write Model (tabela `events`)

**Co to jest?**  
PostgreSQL to relacyjna baza danych, która w EventMaster przechowuje **zarówno Write Model jak i Read Model** w osobnych tabelach. Najpierw omówimy **Write Model** - tabelę `events`.

**Struktura tabeli `events`:**
```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,             -- Unikalny identyfikator eventu
    title VARCHAR(255) NOT NULL,     -- Tytuł wydarzenia
    description TEXT,                -- Opis wydarzenia
    event_date TIMESTAMP NOT NULL,   -- Data wydarzenia
    organizer_id VARCHAR(255) NOT NULL  -- ID organizatora (z Keycloak)
);
```

**Co przechowuje?**
- **Agregaty domeny** - Encje `Event` z pełną informacją
- **Indeksy** - Optymalizowane pod zapis (PRIMARY KEY)
- **Constraints** - Reguły integralności (NOT NULL, UNIQUE)

**Kto zapisuje dane?**
- Tylko `EventCommandHandler` (przez `EventRepository`)
- Tabela zarządzana przez **Flyway migrations**

**Lokalizacja migracji:**
`src/main/resources/db/migration/V1__create_events_table.sql`

**Dlaczego PostgreSQL?**
- Silne gwarancje ACID (Atomicity, Consistency, Isolation, Durability)
- Dobre wsparcie dla transakcji
- Flyway do kontrolowanej ewolucji schematu
- Popularny, dobrze wspierany, solidny

**Konfiguracja:**
```java
// WriteDataSourceConfig.java
@EnableJpaRepositories(
    basePackages = "com.eventmaster.backend.events.repository",  // Tylko Write Model Repositories
    entityManagerFactoryRef = "writeEntityManagerFactory",
    transactionManagerRef = "writeTransactionManager"
)
```

---

#### 4.2.6 PostgreSQL - Read Model (tabela `event_view`)

**Co to jest?**  
Ta sama baza PostgreSQL, ale **osobna tabela** `event_view` zoptymalizowana pod odczyty. To uproszczenie architektury - w klasycznym CQRS byłaby to osobna baza danych (np. CockroachDB, Cassandra).

**Dlaczego jedna baza?**
- **Uproszczenie na etapie MVP** - łatwiejsze testowanie i rozwój
- **Nadal CQRS** - separacja logiczna (różne tabele, różne repository, różne DataSource beany)
- **Łatwa migracja** - w produkcji możemy rozdzielić na osobne bazy bez zmian w kodzie

**Struktura tabeli `event_view`:**
```sql
CREATE TABLE event_view (
    event_id UUID PRIMARY KEY,       -- Taki sam ID jak w Write Model
    title VARCHAR(255),              -- Tytuł (denormalizowany)
    description TEXT,                -- Opis (denormalizowany)  
    event_date TIMESTAMP,            -- Data (denormalizowana)
    organizer_id VARCHAR(255)        -- ID organizatora (denormalizowany)
);

-- Możliwe dodatkowe indeksy dla szybkiego wyszukiwania:
CREATE INDEX idx_event_view_date ON event_view(event_date);
CREATE INDEX idx_event_view_organizer ON event_view(organizer_id);
```

**Co przechowuje?**
- **Widoki** (Views/Projekcje) - Uproszczone dane do wyświetlania
- **Denormalizowane dane** - Wszystko w jednej tabeli, bez JOIN-ów

**Kto zapisuje dane?**
- Tylko `EventViewProjector` (przez `EventViewRepository`)
- Tabela zarządzana przez **Hibernate** (`ddl-auto=update`)

**Kto czyta dane?**
- `EventQueryController` (przez `EventViewRepository`)
- Frontend przez endpoint `GET /api/v1/events`

**Eventual Consistency:**
```
[Zapis do events] ---(kilka ms)---> [Redpanda] ---(kilka ms)---> [Zapis do event_view]
      t=0ms                            t=5ms                            t=10ms
```
Użytkownik może nie zobaczyć swojego eventu przez ~10-50ms po utworzeniu.

**Konfiguracja:**
```java
// ReadDataSourceConfig.java
@EnableJpaRepositories(
    basePackages = "com.eventmaster.backend.events.query.repository",  // Tylko Read Model Repositories
    entityManagerFactoryRef = "readEntityManagerFactory",
    transactionManagerRef = "readTransactionManager"
)

// W testach oba DataSource wskazują na ten sam PostgreSQLContainer
```

---

#### 4.2.7 Keycloak - Authorization Server

**Co to jest?**  
Keycloak to serwer autoryzacji implementujący protokoły OAuth2 i OpenID Connect (OIDC). Obsługuje logowanie użytkowników i wydawanie tokenów JWT.

**Co przechowuje?**
- **Użytkownicy** - Login, hasło (hashowane), email
- **Klienty** - Aplikacje mające dostęp (EventMaster Frontend)
- **Realmy** - Izolowane przestrzenie konfiguracji
- **Sesje** - Aktywne sesje użytkowników
- **Tokeny** - Wydane access tokeny i refresh tokeny

**Jak działa przepływ OAuth2?**

```mermaid
sequenceDiagram
    participant User as 👤 Użytkownik
    participant Nuxt as Nuxt Frontend
    participant KC as Keycloak
    participant Spring as Spring Backend
    
    User->>Nuxt: Klik "Zaloguj się"
    Nuxt->>KC: Przekierowanie do /auth (z client_id, redirect_uri)
    KC->>User: Formularz logowania
    User->>KC: Wprowadza login/hasło
    KC->>Nuxt: Callback z authorization code
    Nuxt->>KC: Wymień code na token (POST /token)
    KC->>Nuxt: Zwróć JWT access_token + refresh_token
    Nuxt->>Nuxt: Zapisz token w cookie (httpOnly)
    
    Note over Nuxt,Spring: Teraz użytkownik jest zalogowany
    
    User->>Nuxt: Wypełnia formularz eventu
    Nuxt->>Spring: POST /api/v1/events (Header: Authorization: Bearer {token})
    Spring->>KC: Waliduj token (JWKS endpoint)
    KC->>Spring: Token valid ✅ (+ claims: sub, email, roles)
    Spring->>Spring: Wyciągnij organizerId z token.subject
    Spring->>Nuxt: 202 Accepted
```

**JWT Token - co zawiera?**
```json
{
  "sub": "f6e9a1b2-3c4d-5e6f-7a8b-9c0d1e2f3a4b",  // Subject = user ID
  "email": "jan.kowalski@example.com",
  "preferred_username": "jankowalski",
  "name": "Jan Kowalski",
  "iat": 1697812345,  // Issued at timestamp
  "exp": 1697815945,  // Expiration timestamp (1h)
  "iss": "http://localhost:8180/realms/eventmaster"  // Issuer
}
```

**Jak Spring Boot waliduje token?**
```java
// SecurityConfig.java
@Bean
SecurityFilterChain filterChain(HttpSecurity http) {
    http.oauth2ResourceServer(oauth2 -> 
        oauth2.jwt(jwt -> 
            jwt.jwkSetUri("http://localhost:8180/realms/eventmaster/protocol/openid-connect/certs")
        )
    );
}
```
Spring automatycznie:
1. Pobiera klucze publiczne z Keycloak (JWKS)
2. Waliduje sygnaturę tokenu
3. Sprawdza czy nie wygasł (exp claim)
4. Udostępnia claims przez `@AuthenticationPrincipal Jwt jwt`

---

## 5. Backend Java - szczegółowa analiza

### 5.1 Warstwa API (Controllers)

#### 5.1.1 EventCommandController

**Lokalizacja:** `com.eventmaster.backend.events.api.EventCommandController`

**Rola:** Endpoint HTTP do przyjmowania komend tworzenia eventów.

**Kod z wyjaśnieniami:**
```java
@RestController  // ← To oznacza, że klasa obsługuje HTTP REST API
@RequestMapping("/api/v1/events")  // ← Wszystkie metody mają prefix /api/v1/events
@RequiredArgsConstructor  // ← Lombok: generuje konstruktor z polami final
public class EventCommandController {

    // ← Wstrzykiwany przez Spring (Dependency Injection)
    private final KafkaTemplate<String, CreateEventCommand> kafkaTemplate;
    
    public static final String TOPIC_COMMANDS_EVENTS_CREATE = "commands.events.create";

    @PostMapping  // ← Obsługuje POST /api/v1/events
    public ResponseEntity<Void> createEvent(
            @Valid @RequestBody CreateEventRequest request,  // ← Walidacja Bean Validation
            @AuthenticationPrincipal Jwt jwt  // ← Spring automatycznie wstrzykuje JWT token
    ) {
        // 1. Wyciągamy ID użytkownika z tokenu JWT
        String organizerId = jwt.getSubject();  // "sub" claim z tokenu

        // 2. Tworzymy komendę (immutable record)
        var command = new CreateEventCommand(
            UUID.randomUUID(),       // ← Generujemy nowe UUID dla eventu
            organizerId,             // ← Z tokenu JWT
            request.title(),
            request.description(),
            request.eventDate()
        );

        // 3. Wysyłamy komendę do Kafki
        //    - Topic: "commands.events.create"
        //    - Key: UUID eventu (dla partycjonowania)
        //    - Value: Cała komenda
        kafkaTemplate.send(TOPIC_COMMANDS_EVENTS_CREATE, command.eventId().toString(), command);

        // 4. Zwracamy 202 Accepted (przyjęte do przetworzenia)
        //    NIE CZEKAMY na przetworzenie!
        return ResponseEntity.accepted().build();
    }
}
```

**Przepływ żądania:**
```
1. Frontend → POST /api/v1/events
            Content-Type: application/json
            Authorization: Bearer eyJhbGciOi...
            Body: { "title": "Koncert", "description": "...", "eventDate": "2025-12-31T20:00:00Z" }

2. Spring Security → Waliduje JWT token
3. @Valid → Waliduje request body (title nie pusty, data w przyszłości)
4. Controller → Tworzy CreateEventCommand
5. KafkaTemplate → Wysyła do topicu "commands.events.create"
6. Controller → Zwraca 202 Accepted (KOŃCZY przetwarzanie w kontrolerze)
```

**Dlaczego 202 Accepted, a nie 201 Created?**
- `201 Created` oznaczałby, że zasób już istnieje w systemie
- `202 Accepted` oznacza, że przyjęliśmy żądanie, ale jeszcze go przetwarzamy
- To uczciwe wobec klienta - mówimy prawdę, że dane NIE są jeszcze dostępne

---

#### 5.1.2 EventQueryController

**Lokalizacja:** `com.eventmaster.backend.events.query.api.EventQueryController`

**Rola:** Endpoint HTTP do odczytu listy eventów.

**Kod:**
```java
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventQueryController {

    // ← Połączenie do Read Model (PostgreSQL - tabela event_view)
    private final EventViewRepository eventViewRepository;

    @GetMapping  // ← Obsługuje GET /api/v1/events
    public ResponseEntity<List<EventViewDTO>> getAllEvents() {
        // 1. Pobierz wszystkie EventView z PostgreSQL (tabela event_view)
        List<EventViewDTO> events = eventViewRepository.findAll()
                .stream()
                // 2. Mapuj EventView → EventViewDTO (tylko potrzebne pola)
                .map(eventView -> new EventViewDTO(
                        eventView.getEventId(),
                        eventView.getTitle(),
                        eventView.getEventDate()
                ))
                .collect(Collectors.toList());

        // 3. Zwróć 200 OK z listą
        return ResponseEntity.ok(events);
    }
}
```

**Przepływ żądania:**
```
1. Frontend → GET /api/v1/events
2. Controller → eventViewRepository.findAll()
3. PostgreSQL Read Model → Zwraca wszystkie rekordy z tabeli event_view
4. Stream → Mapuje EventView na EventViewDTO
5. Controller → Zwraca 200 OK z listą JSON
```

**Uwaga:** Ten endpoint **NIE wymaga autoryzacji** - każdy może przeglądać listę eventów (publiczna).

---

### 5.2 Warstwa Command (Write Side)

#### 5.2.1 CreateEventCommand

**Lokalizacja:** `com.eventmaster.backend.events.command.CreateEventCommand`

**Rola:** Immutable command object (record) reprezentujący intencję użytkownika.

```java
public record CreateEventCommand(
    UUID eventId,        // ← Wygenerowane przez kontroler
    String organizerId,  // ← Z JWT tokenu (sub claim)
    String title,
    String description,
    Instant eventDate
) {}
```

**Dlaczego `record`?**
- Records są immutable (niezmienalne) - raz utworzone, nie można zmienić
- Automatyczne generowanie: `equals()`, `hashCode()`, `toString()`, gettery
- Idealne dla DTO i komend

**Serializacja do Kafki:**
```json
{
  "eventId": "f6e9a1b2-3c4d-5e6f-7a8b-9c0d1e2f3a4b",
  "organizerId": "user-123",
  "title": "Koncert Rockowy",
  "description": "Najlepsze zespoły roku",
  "eventDate": "2025-12-31T20:00:00Z"
}
```
Spring Kafka automatycznie serializuje do JSON używając Jackson.

---

#### 5.2.2 EventCommandHandler

**Lokalizacja:** `com.eventmaster.backend.events.EventCommandHandler`

**Rola:** Konsument Kafka, który przetwarza komendy i zapisuje do Write Model.

**Pełny kod z wyjaśnieniami:**
```java
@Slf4j  // ← Lombok: automatyczny logger
@Service
@RequiredArgsConstructor
public class EventCommandHandler {

    public static final String TOPIC_COMMANDS_EVENTS_CREATE = "commands.events.create";
    public static final String TOPIC_EVENTS_LIFECYCLE = "events.lifecycle";  // ← Uproszczona nazwa topicu

    private final EventRepository eventRepository;  // ← Write Model Repository (PostgreSQL)
    private final KafkaTemplate<String, Object> kafkaTemplate;  // ← Do publikacji domain events

    // ← Kafka Listener - subskrybuje topic
    @KafkaListener(
        topics = TOPIC_COMMANDS_EVENTS_CREATE,  // ← Topic z komendami
        groupId = "event-command-handler"       // ← Consumer group ID
    )
    @Transactional  // ← KLUCZOWE! Zapewnia atomowość zapisu do DB
    public void handleCreateEventCommand(CreateEventCommand command) {
        log.info("Received command to create event: {}", command.eventId());

        // === KROK 1: Mapowanie komendy na encję (Write Model) ===
        Event event = Event.builder()
                .id(command.eventId())
                .title(command.title())
                .description(command.description())
                .eventDate(command.eventDate())
                .organizerId(command.organizerId())
                .build();

        // === KROK 2: Utrwalenie w bazie danych (PostgreSQL) ===
        try {
            eventRepository.save(event);
            log.info("Event {} saved to Write Model (Postgres)", event.getId());
        } catch (Exception e) {
            // Np. DataIntegrityViolationException (UUID już istnieje)
            log.error("Failed to save event {} to database. Error: {}", 
                     command.eventId(), e.getMessage());
            // Rzucamy wyjątek → Kafka Consumer spróbuje ponownie
            throw new RuntimeException("Database persistence failed, triggering retry", e);
        }

        // === KROK 3: Stworzenie Domain Event ===
        EventCreatedEvent domainEvent = new EventCreatedEvent(
                command.eventId(),
                command.title(),
                command.description(),
                command.eventDate(),
                command.organizerId()
        );

        // === KROK 4: Publikacja Domain Event do nowego topicu ===
        kafkaTemplate.send(TOPIC_EVENTS_LIFECYCLE,  // ← Uproszczona nazwa topicu
                          command.eventId().toString(), 
                          domainEvent);
        log.info("Published Domain Event {} for event {}", 
                domainEvent.getClass().getSimpleName(), 
                domainEvent.eventId());
    }
}
```

**Dlaczego @Transactional?**
```java
@Transactional
public void handleCreateEventCommand(...) {
    // 1. eventRepository.save() - Zapisz do PostgreSQL
    // 2. kafkaTemplate.send()    - Wyślij domain event do Redpanda
}
```
PROBLEM: Co jeśli `save()` się powiedzie, ale `send()` rzuci wyjątek?
- Event będzie w PostgreSQL Write Model, ale NIE w Read Model (bo brak domain eventu)
- System będzie w niespójnym stanie!

Dzięki `@Transactional`:
- Jeśli `send()` rzuci wyjątek, cała transakcja jest wycofywana (rollback)
- `save()` jest cofnięte
- Kafka Consumer spróbuje ponownie całą operację

**Retry mechanism:**
Jeśli metoda rzuci wyjątek:
1. Kafka Consumer czeka 1 sekundę (FixedBackOff)
2. Próbuje ponownie (maksymalnie 2 razy)
3. Jeśli nadal błąd → wiadomość trafia do Dead Letter Queue (DLQ)

---

#### 5.2.3 Event (Write Model Entity)

**Lokalizacja:** `com.eventmaster.backend.events.domain.Event`

**Rola:** Agregat domenowy w Write Model.

```java
@Data  // ← Lombok: gettery, settery, equals, hashCode, toString
@Builder  // ← Lombok: wzorzec budowniczego
@NoArgsConstructor  // ← Wymagane przez JPA
@AllArgsConstructor
@Entity  // ← JPA: mapowana na tabelę
@Table(name = "events")
public class Event {

    @Id  // ← Primary key
    private UUID id;
    
    private String title;
    private String description;
    private Instant eventDate;
    private String organizerId;
}
```

**Mapowanie na tabelę:**
```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    event_date TIMESTAMP,
    organizer_id VARCHAR(255)
);
```

**Flyway migration** (PostgreSQL):
Lokalizacja: `src/main/resources/db/migration/V1__create_events_table.sql`

---

#### 5.2.4 EventRepository

**Lokalizacja:** `com.eventmaster.backend.events.repository.EventRepository`

```java
public interface EventRepository extends JpaRepository<Event, UUID> {
    // Spring Data JPA automatycznie implementuje:
    // - save(Event)
    // - findById(UUID)
    // - findAll()
    // - delete(Event)
    // itd.
}
```

**Jak to działa?**
Spring Data JPA w runtime generuje implementację na podstawie:
1. Nazwy interfejsu
2. Typu encji (`Event`)
3. Typu ID (`UUID`)

Pod spodem używa Hibernate do wykonywania SQL-i.

---

### 5.3 Warstwa Domain Events

#### 5.3.1 EventCreatedEvent

**Lokalizacja:** `com.eventmaster.backend.events.domain.EventCreatedEvent`

**Rola:** Domain Event informujący, że event został utworzony.

```java
public record EventCreatedEvent(
    UUID eventId,
    String title,
    String description,
    Instant eventDate,
    String organizerId
) {}
```

**Różnica między Command a Domain Event:**

| Aspekt | Command | Domain Event |
|--------|---------|--------------|
| **Intencja** | "Chcę utworzyć event" | "Event został utworzony" |
| **Czas** | Przyszłość / Imperatyw | Przeszłość / Past tense |
| **Nazwa** | CreateEventCommand | EventCreatedEvent |
| **Może być odrzucona?** | Tak (walidacja) | Nie (już się stało) |
| **Źródło** | Frontend/User | Backend/Domain |
| **Konsumenci** | Command Handler | Projectors, inne usługi |

**Przykładowy przepływ:**
```
1. User klika "Utwórz" → CreateEventCommand
2. Command Handler waliduje i zapisuje → Sukces
3. Command Handler emituje → EventCreatedEvent
4. EventViewProjector odbiera event i aktualizuje Read Model
5. [Potencjalnie] EmailService odbiera event i wysyła powiadomienie
6. [Potencjalnie] AuditService odbiera event i loguje do audytu
```

Domain Events pozwalają na **luźne sprzężenie** (loose coupling) - możemy dodawać nowe handlery bez zmiany istniejącego kodu.

---


### 5.4 Warstwa Query (Read Side)

#### 5.4.1 EventViewProjector

**Lokalizacja:** `com.eventmaster.backend.events.query.projector.EventViewProjector`

**Rola:** Redpanda Consumer, który projektuje Domain Events do Read Model (PostgreSQL - tabela event_view).

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class EventViewProjector {

    public static final String TOPIC_DOMAIN_EVENTS_LIFECYCLE = "events.lifecycle";

    private final EventViewRepository eventViewRepository;  // ← Read Model Repository

    @KafkaListener(
        topics = TOPIC_DOMAIN_EVENTS_LIFECYCLE,  // ← Topic z domain events
        groupId = "eventmaster-projectors-crdb"  // ← Osobna grupa konsumentów (nazwa historyczna)
    )
    public void handleEventCreated(EventCreatedEvent event) {
        log.info("Projecting EventCreatedEvent to Read Model (event_view): {}", event.eventId());

        // === Mapowanie Domain Event → Read Model ===
        EventView readModel = new EventView();
        readModel.setEventId(event.eventId());
        readModel.setTitle(event.title());
        readModel.setDescription(event.description());
        readModel.setEventDate(event.eventDate());
        readModel.setOrganizerId(event.organizerId());

        // === Zapis do PostgreSQL Read Model ===
        try {
            eventViewRepository.save(readModel);
            log.info("Event View {} saved to Read Model (event_view)", readModel.getEventId());
        } catch (Exception e) {
            log.error("Failed to save Event View {} to Read Model. Error: {}", 
                     event.eventId(), e.getMessage());
            // Rzucamy wyjątek → Redpanda spróbuje ponownie
            throw new RuntimeException("Read Model persistence failed, triggering retry", e);
        }
    }
}
```

**Co to jest projekcja?**
Projekcja to proces transformacji danych z jednego modelu do drugiego. Analogia:
- **Write Model** to jak surowe dane ankiety (pełne odpowiedzi, metadane, timestampy)
- **Read Model** to jak wykres podsumowujący wyniki ankiety (uproszczony, gotowy do wyświetlenia)

**Dlaczego osobna grupa konsumentów?**
```
Consumer Group: event-command-handler
  ├─ Przetwarza: CreateEventCommand
  └─ Pisze do: PostgreSQL (Write Model)

Consumer Group: eventmaster-projectors-crdb (nazwa historyczna)
  ├─ Przetwarza: EventCreatedEvent
  └─ Pisze do: PostgreSQL Read Model (tabela event_view)
```
Dzięki różnym groupId, obie grupy niezależnie konsumują te same wiadomości.

---

#### 5.4.2 EventView (Read Model Entity)

**Lokalizacja:** `com.eventmaster.backend.events.query.model.EventView`

```java
@Entity
@Table(name = "event_view")  // ← Nazwa tabeli w PostgreSQL
public class EventView {
    @Id
    private UUID eventId;
    private String title;
    private String description;
    private Instant eventDate;
    private String organizerId;
    
    // Gettery i settery...
}
```

**Dlaczego prosta struktura?**
Read Model jest denormalizowany - wszystkie dane w jednej tabeli, żeby odczyt był ultra-szybki:
```sql
-- Jedna prosta SELECT bez JOIN-ów:
SELECT * FROM event_view ORDER BY event_date DESC;
```

W przeciwieństwie do Write Model, gdzie moglibyśmy mieć:
```sql
-- Write Model z relacjami (hipotetycznie):
SELECT e.*, o.name, o.email 
FROM events e
JOIN organizers o ON e.organizer_id = o.id
WHERE e.event_date > NOW();
```

---

#### 5.4.3 EventViewDTO

**Lokalizacja:** `com.eventmaster.backend.events.query.dto.EventViewDTO`

```java
public record EventViewDTO(
    UUID eventId,
    String title,
    Instant eventDate
    // ← Tylko pola potrzebne na liście!
) {}
```

**Dlaczego osobny DTO?**
- **EventView** - Pełna encja z bazy (wszystkie kolumny)
- **EventViewDTO** - Tylko dane wysyłane do frontendu

Korzyści:
1. **Mniej danych** - Nie wysyłamy `description` i `organizerId`, jeśli frontend nie potrzebuje
2. **Bezpieczeństwo** - Możemy ukryć wrażliwe dane
3. **Stabilność API** - Możemy zmienić EventView bez zmiany API

---

### 5.5 Warstwa konfiguracji

#### 5.5.1 WriteDataSourceConfig

**Lokalizacja:** `com.eventmaster.backend.configs.persistence.WriteDataSourceConfig`

**Rola:** Konfiguruje połączenie do PostgreSQL dla Write Model.

```java
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = "com.eventmaster.backend.events.repository",  // ← Tylko EventRepository
    entityManagerFactoryRef = "writeEntityManagerFactory",
    transactionManagerRef = "writeTransactionManager"
)
public class WriteDataSourceConfig {

    @Primary  // ← Domyślny DataSource (gdy nie określimy @Qualifier)
    @Bean(name = "writeDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.write")  // ← Czyta z application.yml
    public DataSource writeDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Primary
    @Bean(name = "writeEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean writeEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("writeDataSource") DataSource dataSource
    ) {
        return builder
                .dataSource(dataSource)
                .packages("com.eventmaster.backend.events.domain")  // ← Tylko Event.java
                .persistenceUnit("write")
                .build();
    }

    @Primary
    @Bean(name = "writeTransactionManager")
    public PlatformTransactionManager writeTransactionManager(
            @Qualifier("writeEntityManagerFactory") EntityManagerFactory entityManagerFactory
    ) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
```

**Konfiguracja w `application.yml`:**
```yaml
spring:
  datasource:
    write:
      url: jdbc:postgresql://localhost:5432/eventmaster_db
      username: user
      password: password
      driver-class-name: org.postgresql.Driver
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```

**Co się dzieje pod spodem?**
1. Spring Boot czyta konfigurację `spring.datasource.write.*`
2. Tworzy `DataSource` (connection pool do PostgreSQL)
3. Tworzy `EntityManagerFactory` dla pakietu `events.domain`
4. Tworzy `JpaTransactionManager` do zarządzania transakcjami

---

#### 5.5.2 ReadDataSourceConfig

**Lokalizacja:** `com.eventmaster.backend.configs.persistence.ReadDataSourceConfig`

**Rola:** Konfiguruje drugi DataSource bean dla Read Model. W produkcji może wskazywać na osobną bazę danych, w MVP wskazuje na te samą PostgreSQL co Write Model.

**Uwaga:** W aktualnej implementacji (MVP), oba DataSource beany wskazują na tę samą bazę PostgreSQL, ale na różne tabele (`events` vs `event_view`). To uproszczenie, które można łatwo zmienić później na osobne bazy danych.

```java
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = "com.eventmaster.backend.events.query.repository",  // ← Tylko EventViewRepository
    entityManagerFactoryRef = "readEntityManagerFactory",
    transactionManagerRef = "readTransactionManager"
)
public class ReadDataSourceConfig {

    @Bean(name = "readDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.read")
    public DataSource readDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "readEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean readEntityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier("readDataSource") DataSource dataSource
    ) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "update");  // ← Auto-update schematu (uproszczenie MVP)

        return builder
                .dataSource(dataSource)
                .packages("com.eventmaster.backend.events.query.model")  // ← Tylko EventView.java
                .persistenceUnit("read")
                .properties(properties)
                .build();
    }

    @Bean(name = "readTransactionManager")
    public PlatformTransactionManager readTransactionManager(
            @Qualifier("readEntityManagerFactory") EntityManagerFactory entityManagerFactory
    ) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
```

**Konfiguracja w `application.properties` (dla MVP - ta sama baza):**
```properties
# Read Model DataSource (MVP: ta sama baza co Write Model)
spring.datasource.read.url=jdbc:postgresql://localhost:5432/eventmaster_db
spring.datasource.read.username=user
spring.datasource.read.password=password
spring.datasource.read.driver-class-name=org.postgresql.Driver
```

**Konfiguracja dla produkcji (osobna baza):**
```properties
# Write Model
spring.datasource.write.url=jdbc:postgresql://postgres-write.example.com:5432/eventmaster_write
spring.datasource.write.username=user
spring.datasource.write.password=secret

# Read Model (osobny serwer - może być MongoDB, CockroachDB, etc.)
spring.datasource.read.url=jdbc:postgresql://cockroachdb.example.com:26257/eventmaster_read?sslmode=require
spring.datasource.read.username=root
spring.datasource.read.password=secret
```

**Dlaczego `hibernate.hbm2ddl.auto=update`?**
- Write Model używa **Flyway** do migracji (kontrolowane przez devów, versioned)
- Read Model używa **Hibernate auto-update** (generowane automatycznie z encji)
- To akceptowalne, bo Read Model jest mniej krytyczny i łatwiej go przebudować z Domain Events
- W produkcji można też użyć Flyway dla Read Model

**Jak działa separacja logiczna?**
```
EventRepository (Write)
  └─> writeEntityManagerFactory
      └─> writeDataSource
          └─> PostgreSQL:5432/eventmaster_db (tabela: events)

EventViewRepository (Read)
  └─> readEntityManagerFactory
      └─> readDataSource
          └─> PostgreSQL:5432/eventmaster_db (tabela: event_view)
                     ↑
              Ta sama baza w MVP!
```

Dzięki takiej konfiguracji:
- ✅ Kod nie wie, że bazy są te same (łatwa migracja później)
- ✅ Repositories są oddzielone logicznie
- ✅ Testy mogą łatwo używać jednego PostgreSQLContainer

---

#### 5.5.3 KafkaConfig

**Lokalizacja:** `com.eventmaster.backend.config.KafkaConfig`

**Rola:** Konfiguruje obsługę błędów w Kafka Consumers.

```java
@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(template),  // ← DLQ: gdzie trafiają failed messages
                new FixedBackOff(1000L, 2)  // ← Retry: czekaj 1s, maksymalnie 2 próby
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);  // ← Podpinamy error handler
        return factory;
    }
}
```

**Co to jest Dead Letter Queue (DLQ)?**
Wyobraź sobie pocztę:
1. List przychodzi do skrzynki
2. Kurier próbuje dostarczyć (1. próba)
3. Nikogo nie ma w domu → Kurier próbuje ponownie następnego dnia (2. próba)
4. Nadal nikogo nie ma → List trafia do "Listy nieodebranych" (DLQ)

W Kafce:
```
Message → Consumer → BŁĄD → Retry (1s) → BŁĄD → Retry (1s) → BŁĄD → DLQ Topic
```

DLQ Topic: `{original-topic}.DLT` (np. `commands.events.create.DLT`)

Można później ręcznie przeanalizować błędne wiadomości i naprawić problem.

---

#### 5.5.4 SecurityConfig

**Lokalizacja:** `com.eventmaster.backend.config.SecurityConfig`

**Rola:** Konfiguruje zabezpieczenia OAuth2 Resource Server.

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/v1/events").permitAll()  // ← GET bez autoryzacji
                .requestMatchers(HttpMethod.POST, "/api/v1/events").authenticated()  // ← POST wymaga tokenu
                .requestMatchers("/actuator/health").permitAll()  // ← Health check publiczny
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt
                    .jwkSetUri("http://localhost:8180/realms/eventmaster/protocol/openid-connect/certs")
                )
            )
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            .csrf(csrf -> csrf.disable());  // ← Wyłączamy CSRF (REST API używa JWT)

        return http.build();
    }

    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOrigins(List.of("http://localhost:3000"));  // ← Frontend Nuxt
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE"));
        config.setAllowedHeaders(List.of("*"));
        config.setAllowCredentials(true);  // ← Zezwól na cookies/auth headers

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return source;
    }
}
```

**Co robi Spring Security z tym configiem?**

Dla każdego żądania HTTP:
1. **CORS Filter**: Sprawdza czy origin jest dozwolony
2. **Authorization Filter**: 
   - Dla `/actuator/health` → Przepuść
   - Dla `GET /api/v1/events` → Przepuść
   - Dla `POST /api/v1/events` → Sprawdź token:
     - Wyciągnij z header `Authorization: Bearer {token}`
     - Pobierz klucze publiczne z Keycloak (JWKS endpoint)
     - Zwaliduj sygnaturę JWT
     - Sprawdź czy nie wygasł
     - Jeśli OK → Wstrzyknij `Jwt` do controllera
     - Jeśli NIE → Zwróć `401 Unauthorized`

---

## 6. Frontend Nuxt.js - szczegółowa analiza

### 6.1 Struktura projektu

```
frontend/
├── app/
│   └── app.vue                    # Root component
├── pages/
│   ├── index.vue                  # Strona główna (/)
│   └── events/
│       ├── index.vue              # Lista eventów (/events)
│       └── create.vue             # Tworzenie eventu (/events/create)
├── server/
│   └── api/
│       └── auth/
│           └── [...].ts           # Callback handler OAuth2
├── nuxt.config.ts                 # Konfiguracja Nuxt
└── package.json                   # Dependencies
```

---

### 6.2 Strony (Pages)

#### 6.2.1 /pages/events/index.vue

**Rola:** Wyświetla listę wszystkich eventów (pobiera z Read Model).

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue';

// === Reactive State ===
const events = ref([]);  // ← Tablica eventów (reactive)
const loading = ref(true);  // ← Czy trwa ładowanie?

// === Lifecycle Hook ===
onMounted(async () => {
  try {
    // Pobierz dane z backendu
    const response = await fetch('/api/v1/events');
    
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    
    // Rozpakuj JSON i zapisz do state
    events.value = await response.json();
  } catch (error) {
    console.error('There was a problem with the fetch operation:', error);
    // TODO: Pokazać error użytkownikowi (toast/alert)
  } finally {
    loading.value = false;  // ← Zawsze wyłącz loading
  }
});
</script>

<template>
  <div>
    <h1>Events</h1>
    
    <!-- Loading spinner -->
    <div v-if="loading">
      <p>Ładowanie...</p>
    </div>
    
    <!-- Lista eventów -->
    <div v-else>
      <div v-if="events.length > 0">
        <ul>
          <li v-for="event in events" :key="event.eventId">
            <h2>{{ event.title }}</h2>
            <p>{{ new Date(event.eventDate).toLocaleString() }}</p>
          </li>
        </ul>
      </div>
      
      <!-- Brak eventów -->
      <div v-else>
        <p>Aktualnie nie ma żadnych nadchodzących wydarzeń.</p>
      </div>
    </div>
  </div>
</template>
```

**Przepływ:**
```mermaid
sequenceDiagram
    participant Browser
    participant Vue
    participant Caddy
    participant Spring
    participant ReadDB as PostgreSQL Read Model
    
    Browser->>Vue: Otwiera /events
    Vue->>Vue: onMounted() hook
    Vue->>Caddy: GET /api/v1/events
    Caddy->>Spring: Proxy do /api/v1/events
    Spring->>ReadDB: SELECT * FROM event_view
    ReadDB->>Spring: Zwróć rows
    Spring->>Caddy: 200 OK + JSON array
    Caddy->>Vue: JSON array
    Vue->>Vue: events.value = data
    Vue->>Browser: Renderuj listę
```

**Dlaczego `fetch('/api/v1/events')`?**
- Nuxt automatycznie proxy przez Caddy
- Nie musimy hardcode'ować `http://localhost:8080/api/v1/events`
- To działa zarówno na localhost, jak i na produkcji

---

#### 6.2.2 /pages/events/create.vue

**Rola:** Formularz tworzenia nowego eventu.

```vue
<script setup lang="ts">
import { ref } from 'vue';
import { useToast } from 'primevue/usetoast';
import { z } from 'zod';
import { useForm } from 'vee-validate';
import { toTypedSchema } from '@vee-validate/zod';
import InputText from 'primevue/inputtext';
import Editor from 'primevue/editor';
import Calendar from 'primevue/calendar';
import Button from 'primevue/button';

// === Page Meta ===
definePageMeta({
  middleware: 'auth'  // ← Tylko zalogowani użytkownicy
});

// === Composables ===
const toast = useToast();
const router = useRouter();
const isLoading = ref(false);

// === Validation Schema (Zod) ===
const CreateEventSchema = z.object({
  title: z.string().nonempty('Tytuł nie może być pusty'),
  description: z.string().optional(),
  eventDate: z.date().min(new Date(), 'Data musi być w przyszłości'),
});

type CreateEventRequest = z.infer<typeof CreateEventSchema>;

// === Form Setup (vee-validate) ===
const { handleSubmit, defineInputBinds, errors } = useForm({
  validationSchema: toTypedSchema(CreateEventSchema),
});

const title = defineInputBinds('title');
const description = defineInputBinds('description');
const eventDate = defineInputBinds('eventDate');

// === Submit Handler ===
const onSubmit = handleSubmit(async (formData: CreateEventRequest) => {
    isLoading.value = true;
    
    try {
        // Wyślij POST do backendu
        await $fetch('/api/v1/events', {
            method: 'POST',
            body: {
              ...formData,
              eventDate: formData.eventDate.toISOString()  // ← Konwersja do ISO-8601
            },
        });

        // Sukces - pokaż toast
        toast.add({
            severity: 'success',
            summary: 'Przyjęto',
            detail: 'Twoje wydarzenie jest przetwarzane.',
            life: 3000
        });
        
        // Przekieruj na stronę główną
        router.push('/');

    } catch (error) {
        // Błąd - pokaż toast
        toast.add({
            severity: 'error',
            summary: 'Błąd',
            detail: 'Nie udało się przyjąć polecenia.',
            life: 3000
        });
    } finally {
        isLoading.value = false;
    }
});
</script>

<template>
  <div class="p-card p-4">
    <h1 class="text-2xl font-bold mb-4">Utwórz Nowe Wydarzenie</h1>
    
    <form @submit.prevent="onSubmit" class="flex flex-col gap-4">
      <!-- Tytuł -->
      <div class="flex flex-col">
        <label for="title" class="mb-2">Tytuł</label>
        <InputText id="title" v-bind="title" />
        <small class="p-error">{{ errors.title }}</small>
      </div>
      
      <!-- Opis (WYSIWYG editor) -->
      <div class="flex flex-col">
        <label for="description" class="mb-2">Opis</label>
        <Editor id="description" v-bind="description" editorStyle="height: 320px" />
        <small class="p-error">{{ errors.description }}</small>
      </div>
      
      <!-- Data wydarzenia (Calendar picker) -->
      <div class="flex flex-col">
        <label for="eventDate" class="mb-2">Data wydarzenia</label>
        <Calendar id="eventDate" v-bind="eventDate" />
        <small class="p-error">{{ errors.eventDate }}</small>
      </div>
      
      <!-- Submit button -->
      <Button type="submit" label="Utwórz" :loading="isLoading" />
    </form>
  </div>
</template>
```

**Co się dzieje po kliknięciu "Utwórz"?**

```mermaid
sequenceDiagram
    participant User
    participant Vue
    participant Zod
    participant NuxtAuth
    participant Spring
    participant Kafka
    
    User->>Vue: Wypełnia formularz i klika "Utwórz"
    Vue->>Zod: Waliduj dane (vee-validate + Zod)
    
    alt Walidacja failed
        Zod->>Vue: Zwróć błędy
        Vue->>User: Pokaż błędy pod polami
    else Walidacja OK
        Zod->>Vue: Dane OK
        Vue->>NuxtAuth: Pobierz JWT token z session
        Vue->>Spring: POST /api/v1/events (+ Bearer token)
        Spring->>Spring: Waliduj JWT
        Spring->>Kafka: Wyślij CreateEventCommand
        Spring->>Vue: 202 Accepted
        Vue->>User: Toast "Przyjęto"
        Vue->>User: Przekieruj na /
    end
```

**Walidacja Zod:**
```typescript
z.date().min(new Date(), 'Data musi być w przyszłości')
```
To sprawdza, czy wybrana data jest **po** bieżącej dacie. Jeśli nie - pokazuje błąd.

**PrimeVue komponenty:**
- `InputText` - Pole tekstowe
- `Editor` - WYSIWYG editor (Quill.js pod spodem)
- `Calendar` - Date picker
- `Button` - Przycisk z loading spinner
- `Toast` - Powiadomienia (toasty)

---

### 6.3 Middleware i Auth

#### 6.3.1 Middleware: auth

**Definicja w `definePageMeta`:**
```typescript
definePageMeta({
  middleware: 'auth'  // ← Nuxt automatycznie ładuje middleware z @sidebase/nuxt-auth
});
```

**Co robi middleware `auth`?**
```typescript
// Pseudo-kod (wewnętrzna logika @sidebase/nuxt-auth)
export default defineNuxtRouteMiddleware((to, from) => {
  const { status, signIn } = useAuth();
  
  // Sprawdź czy użytkownik jest zalogowany
  if (status.value === 'unauthenticated') {
    // NIE jest zalogowany → przekieruj do logowania
    return signIn();  // ← To przekierowuje do Keycloak
  }
  
  // Jest zalogowany → przepuść
  return;
});
```

**Przepływ logowania:**
```
1. User → Otwiera /events/create
2. Middleware → Sprawdza status
3. Status = unauthenticated → signIn()
4. signIn() → Przekierowuje do Keycloak
5. Keycloak → Formularz logowania
6. User → Wpisuje login/hasło
7. Keycloak → Callback do /api/auth/callback?code=ABC123
8. Nuxt Server → Wymienia code na token
9. Nuxt Server → Zapisuje token w session cookie
10. Nuxt Server → Przekierowuje z powrotem do /events/create
11. Middleware → Sprawdza status (teraz authenticated)
12. Middleware → Przepuszcza
13. User → Widzi formularz
```

---

#### 6.3.2 Konfiguracja auth w nuxt.config.ts

**Lokalizacja:** `frontend/nuxt.config.ts`

```typescript
export default defineNuxtConfig({
  modules: ['@sidebase/nuxt-auth'],
  
  auth: {
    provider: {
      type: 'authjs',  // ← Używamy Auth.js (NextAuth)
    },
    globalAppMiddleware: false,  // ← Nie wszystkie strony wymagają logowania
  },
  
  runtimeConfig: {
    auth: {
      secret: process.env.NUXT_AUTH_SECRET || 'super-tajny-sekret-zmien-mnie',
    },
    public: {
      auth: {
        // Konfiguracja OAuth2 (Keycloak)
        keycloak: {
          clientId: 'eventmaster-frontend',
          clientSecret: process.env.KEYCLOAK_CLIENT_SECRET,
          issuer: 'http://localhost:8180/realms/eventmaster',
        }
      }
    }
  }
});
```

---

#### 6.3.3 Server API: /server/api/auth/[...].ts

**Rola:** Catch-all endpoint obsługujący OAuth2 callbacks i token management.

```typescript
// frontend/server/api/auth/[...].ts
import { NuxtAuthHandler } from '#auth';
import KeycloakProvider from 'next-auth/providers/keycloak';

export default NuxtAuthHandler({
  secret: useRuntimeConfig().auth.secret,
  
  providers: [
    // @ts-expect-error
    KeycloakProvider.default({
      clientId: 'eventmaster-frontend',
      clientSecret: process.env.KEYCLOAK_CLIENT_SECRET || '',
      issuer: 'http://localhost:8180/realms/eventmaster',
    })
  ],
  
  callbacks: {
    // Callback wywoływany po otrzymaniu tokenu z Keycloak
    async jwt({ token, account }) {
      if (account) {
        // Zapisz access_token do JWT session
        token.accessToken = account.access_token;
      }
      return token;
    },
    
    // Callback wywoływany przy każdym żądaniu useAuth()
    async session({ session, token }) {
      // Dodaj accessToken do obiektu session (dostępny w komponencie)
      session.accessToken = token.accessToken;
      return session;
    }
  }
});
```

**Co się dzieje w callbacks?**

1. **jwt callback**:
   - Wywołany raz po zalogowaniu
   - Otrzymuje `account` z access_tokenem z Keycloak
   - Zapisuje token do JWT session (cookie httpOnly)

2. **session callback**:
   - Wywołany przy każdym `useAuth()` w komponencie
   - Wyciąga token z JWT i dodaje do obiektu session
   - Dzięki temu możemy robić: `const { data: session } = useAuth()` i mieć dostęp do `session.accessToken`

**Automatyczne dołączanie tokenu do requestów:**
```typescript
// Nuxt automatycznie (przez @sidebase/nuxt-auth) dodaje interceptor:
$fetch.interceptors.request = (request) => {
  const { data: session } = useAuth();
  if (session?.accessToken) {
    request.headers.set('Authorization', `Bearer ${session.accessToken}`);
  }
  return request;
};
```

---

## 7. Przepływ danych - scenariusze krok po kroku

### 7.1 Scenariusz 1: Użytkownik tworzy nowy event

**Diagram sekwencji (pełny przepływ):**

```mermaid
sequenceDiagram
    autonumber
    participant User as 👤 Użytkownik
    participant Browser as 🌐 Przeglądarka
    participant Nuxt as Nuxt Frontend
    participant Caddy as Caddy Proxy
    participant KC as Keycloak
    participant Spring as Spring Backend
    participant Redpanda as Redpanda (Message Broker)
    participant WriteDB as PostgreSQL Write Model
    participant CH as EventCommandHandler
    participant Proj as EventViewProjector
    participant ReadDB as PostgreSQL Read Model
    
    %% === LOGOWANIE ===
    User->>Browser: Otwiera /events/create
    Browser->>Nuxt: GET /events/create
    Nuxt->>Nuxt: Middleware sprawdza auth
    Nuxt->>KC: Redirect do /auth (brak tokenu)
    KC->>User: Formularz logowania
    User->>KC: Login + hasło
    KC->>Nuxt: Callback /api/auth/callback?code=ABC
    Nuxt->>KC: POST /token (wymień code)
    KC->>Nuxt: access_token + refresh_token (JWT)
    Nuxt->>Browser: Set-Cookie (session), redirect /events/create
    
    %% === FORMULARZ ===
    Browser->>Nuxt: GET /events/create (z cookie)
    Nuxt->>Browser: Renderuj formularz
    User->>Browser: Wypełnia formularz (tytuł, opis, data)
    User->>Browser: Klik "Utwórz"
    
    %% === WALIDACJA ===
    Browser->>Nuxt: Submit (JavaScript)
    Nuxt->>Nuxt: Zod validation
    
    %% === WYSYŁANIE KOMENDY ===
    Nuxt->>Caddy: POST /api/v1/events<br/>{title, description, eventDate}<br/>Authorization: Bearer eyJhbG...
    Caddy->>Spring: Proxy POST /api/v1/events
    Spring->>KC: Waliduj JWT (JWKS)
    KC->>Spring: Token valid ✅ + subject=user-123
    Spring->>Spring: EventCommandController<br/>Tworzy CreateEventCommand
    Spring->>Redpanda: Publish do "commands.events.create"<br/>Key: event-uuid<br/>Value: {eventId, organizerId, ...}
    Spring->>Caddy: 202 Accepted
    Caddy->>Nuxt: 202 Accepted
    Nuxt->>Browser: Toast "Przyjęto"<br/>Redirect do /
    
    %% === PRZETWARZANIE KOMENDY ===
    Note over Redpanda,CH: Asynchroniczne przetwarzanie
    Redpanda->>CH: Consume CreateEventCommand
    CH->>CH: handleCreateEventCommand()<br/>Mapuje na Event entity
    CH->>WriteDB: INSERT INTO events (...)
    WriteDB->>CH: OK (zapis sukces)
    CH->>CH: Tworzy EventCreatedEvent
    CH->>Redpanda: Publish do "events.lifecycle"
    
    %% === PROJEKCJA DO READ MODEL ===
    Redpanda->>Proj: Consume EventCreatedEvent
    Proj->>Proj: handleEventCreated()<br/>Mapuje na EventView
    Proj->>ReadDB: INSERT INTO event_view (...)
    ReadDB->>Proj: OK (projekcja sukces)
    
    %% === ODCZYT LISTY ===
    Note over User,ReadDB: Użytkownik odświeża listę
    User->>Browser: Otwiera /events
    Browser->>Nuxt: GET /events
    Nuxt->>Caddy: GET /api/v1/events
    Caddy->>Spring: Proxy GET /api/v1/events
    Spring->>ReadDB: SELECT * FROM event_view
    ReadDB->>Spring: Zwróć rows
    Spring->>Caddy: 200 OK + JSON array
    Caddy->>Nuxt: JSON array
    Nuxt->>Browser: Renderuj listę (w tym nowy event ✅)
```

**Czasy (przybliżone):**
- Krok 1-12 (Logowanie): ~2-3 sekundy
- Krok 13-22 (Wysyłanie komendy): ~50-100ms
- Krok 23-27 (Przetwarzanie w tle): ~10-50ms
- Krok 28-31 (Projekcja): ~10-50ms
- **CAŁKOWITY CZAS (logowanie → widoczny event): ~3-4 sekundy**
- **CZAS (submit → 202 Accepted): ~100ms**

---

### 7.2 Scenariusz 2: Użytkownik przegląda listę eventów

**Diagram sekwencji:**

```mermaid
sequenceDiagram
    participant User as 👤 Użytkownik
    participant Browser as 🌐 Przeglądarka
    participant Nuxt as Nuxt Frontend
    participant Caddy as Caddy Proxy
    participant Spring as Spring Backend (QueryController)
    participant ReadDB as PostgreSQL Read Model (event_view)
    
    User->>Browser: Otwiera /events
    Browser->>Nuxt: GET /events
    Nuxt->>Nuxt: onMounted() hook
    Nuxt->>Caddy: fetch('/api/v1/events')
    Caddy->>Spring: Proxy GET /api/v1/events
    Spring->>ReadDB: SELECT event_id, title, event_date<br/>FROM event_view<br/>ORDER BY event_date DESC
    ReadDB->>Spring: Rows [<br/>  {eventId: uuid1, title: "Koncert", eventDate: "2025-12-31"},<br/>  {eventId: uuid2, title: "Konferencja", eventDate: "2026-01-15"}<br/>]
    Spring->>Spring: Map EventView → EventViewDTO
    Spring->>Caddy: 200 OK + JSON array
    Caddy->>Nuxt: JSON array
    Nuxt->>Nuxt: events.value = data
    Nuxt->>Browser: Renderuj <li> dla każdego eventu
    Browser->>User: Wyświetla listę ✅
```

**Czas:** ~20-50ms (bardzo szybko, bo Read Model jest zoptymalizowany)

---

### 7.3 Scenariusz 3: Błąd podczas przetwarzania komendy

**Diagram sekwencji (retry + DLQ):**

```mermaid
sequenceDiagram
    participant Redpanda
    participant CH as EventCommandHandler
    participant PG as PostgreSQL
    participant DLQ as Dead Letter Queue
    
    Redpanda->>CH: Consume CreateEventCommand
    CH->>PG: INSERT INTO events (...)
    PG--xCH: ❌ DataIntegrityViolationException<br/>(duplicate UUID)
    CH->>CH: throw RuntimeException
    
    Note over Redpanda,CH: Retry #1 (czekaj 1s)
    
    Redpanda->>CH: Consume CreateEventCommand (ponownie)
    CH->>PG: INSERT INTO events (...)
    PG--xCH: ❌ Nadal duplicate
    CH->>CH: throw RuntimeException
    
    Note over Redpanda,CH: Retry #2 (czekaj 1s)
    
    Redpanda->>CH: Consume CreateEventCommand (ostatnia próba)
    CH->>PG: INSERT INTO events (...)
    PG--xCH: ❌ Nadal duplicate
    CH->>CH: throw RuntimeException
    
    Note over Redpanda,DLQ: Max retries osiągnięte → DLQ
    
    Redpanda->>DLQ: Przenieś message do "commands.events.create.DLT"
    DLQ->>DLQ: Zapisz w DLQ topic (z metadanymi błędu)
```

**Co zrobić z wiadomościami w DLQ?**
1. Monitoring - Alert, że coś poszło nie tak
2. Ręczna analiza - Developer sprawdza logs i DLQ
3. Naprawa - Fix bug w kodzie
4. Replay - Ponowne przetworzenie z DLQ (jeśli trzeba)

---

## 8. Bazy danych i separacja modeli

### 8.1 Architektura baz danych w EventMaster

**Kluczowe wyjaśnienie:**
EventMaster używa **jednej bazy PostgreSQL** z **dwiema osobnymi tabelami**:
- `events` - Write Model
- `event_view` - Read Model

To uproszczenie klasycznego CQRS, gdzie zwykle są dwie fizycznie oddzielne bazy danych. Nasze podejście:
- ✅ Nadal CQRS (separacja logiczna, różne DataSource beany, różne repositories)
- ✅ Prostsze w testowaniu i developmencie
- ✅ Łatwa migracja do osobnych baz w produkcji
- ✅ Te same korzyści CQRS (optymalizacja, skalowalność logiczna)

---

### 8.2 Write Model (PostgreSQL - tabela `events`)

**Schemat tabeli:**
```sql
CREATE TABLE events (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    event_date TIMESTAMP NOT NULL,
    organizer_id VARCHAR(255) NOT NULL,
    
    -- Constraints
    CONSTRAINT chk_event_date_future 
        CHECK (event_date > CURRENT_TIMESTAMP)
);

-- Indeksy (Write Model ma minimalną liczbę indeksów)
CREATE INDEX idx_events_organizer ON events(organizer_id);
```

**Flyway migration:**
```sql
-- V1__create_events_table.sql
CREATE TABLE events (
    ...
);
```

**Charakterystyka Write Model:**
- **ACID compliance** - Silne gwarancje transakcyjne
- **Normalized** - Może mieć relacje (1:N, N:M)
- **Constraints** - Reguły biznesowe na poziomie DB
- **Minimalne indeksy** - Tylko te potrzebne do lookupów
- **Source of Truth** - Jedyne źródło prawdy o eventach
- **Flyway managed** - Schemat kontrolowany przez migracje

**Operacje:**
- `INSERT` - Tworzenie nowego eventu (EventCommandHandler)
- `UPDATE` - Edycja eventu (potencjalne przyszłe feature)
- `DELETE` - Usuwanie eventu (soft delete preferowane)
- **BRAK** `SELECT` dla query (tylko internal lookups)

---

### 8.3 Read Model (PostgreSQL - tabela `event_view`)

**Schemat tabeli:**
```sql
CREATE TABLE event_view (
    event_id UUID PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    event_date TIMESTAMP,
    organizer_id VARCHAR(255),
    organizer_name VARCHAR(255),  -- ← Denormalizowane (przyszłość)
    organizer_email VARCHAR(255)   -- ← Denormalizowane (przyszłość)
);

-- Indeksy dla szybkich query
CREATE INDEX idx_event_view_date ON event_view(event_date DESC);
CREATE INDEX idx_event_view_organizer ON event_view(organizer_id);
-- Możliwy full-text search:
-- CREATE INDEX idx_event_view_title_search ON event_view USING GIN(to_tsvector('polish', title));
```

**Charakterystyka Read Model:**
- **Denormalized** - Wszystko w jednej tabeli (no JOINs)
- **Eventual consistency** - Może być opóźniony o kilka ms
- **Multiple indexes** - Optymalizowane pod różne query patterns
- **Materialized view concept** - Przechowuje gotowe do wyświetlenia dane
- **Rebuildable** - Można przebudować z domain events
- **Hibernate managed** - Schemat przez ddl-auto=update (uproszczenie MVP)

**Operacje:**
- `SELECT` - Wszystkie query z frontendu (EventQueryController)
- **BRAK** `INSERT/UPDATE/DELETE` ręcznych (tylko przez EventViewProjector)

---

### 8.4 Porównanie modeli

| Aspekt | Write Model (events) | Read Model (event_view) |
|--------|----------------------|-------------------------|
| **Baza** | PostgreSQL | PostgreSQL (ta sama!) |
| **Struktura** | Normalized (3NF) | Denormalized |
| **JOINs** | Tak, przy złożonych relacjach | Nie, wszystko w jednej tabeli |
| **Indeksy** | Minimalne (szybszy zapis) | Wiele (szybszy odczyt) |
| **Constraints** | Silne (NOT NULL, UNIQUE, FK) | Luźne |
| **Operacje** | INSERT, UPDATE, DELETE | SELECT |
| **Zarządzanie schematem** | Flyway migrations | Hibernate ddl-auto |
| **Consistency** | Immediate (ACID) | Eventual (async) |
| **Rebuild** | Trudne (source of truth) | Łatwe (z domain events) |
| **Consistency** | Immediate (ACID) | Eventual (async) |
| **Rebuild** | Trudne (source of truth) | Łatwe (z domain events) |
| **Skalowanie** | Vertical (większa maszyna) | Horizontal (więcej replik) |

---

### 8.5 Dlaczego jedna baza z dwiema tabelami?

**Dlaczego nie dwie osobne bazy (jak w klasycznym CQRS)?**

1. **Uproszczenie MVP** - Łatwiejszy setup, development i testowanie
2. **Koszt** - Nie płacimy za dwie bazy w cloud
3. **Operacyjna prostota** - Jeden backup, jedno połączenie do zarządzania
4. **Testcontainers** - Jeden PostgreSQLContainer w testach
5. **Nadal CQRS** - Separacja logiczna jest zachowana!

**Co tracmy vs klasyczny CQRS?**
- ❌ Niezależna skalowalność baz (nie możemy osobno skalować Read Model)
- ❌ Różne technologie (nie możemy użyć np. MongoDB dla Read Model)
- ❌ Geograficzna replikacja tylko Read Model

**Co zyskujemy?**
- ✅ Prostszy setup i deployment
- ✅ Łatwiejsze testowanie
- ✅ Niższy koszt
- ✅ Łatwa migracja do osobnych baz później (brak zmian w kodzie!)

**Analogia - Biblioteka:**
- **Write Model** (tabela `events`) = Katalog biblioteczny (pełne metadata)
  - Tytuł, autor, ISBN, wydawnictwo, data publikacji, kategoria, lokalizacja na półce
  - Używany przez bibliotekarzy do dodawania/edycji książek
  - Złożona struktura, reguły biznesowe

- **Read Model** (tabela `event_view`) = Wyświetlacz dla czytelników
  - Tylko: Tytuł, autor, dostępność
  - Używany przez czytelników do szukania książek
  - Prosta struktura, szybkie wyszukiwanie

Obie "bazy" są w tej samej "bibliotece" (PostgreSQL), ale mają różne cele!

---

## 9. Redpanda - autobus komunikacyjny

### 9.1 Topici w EventMaster

#### 9.1.1 Topic: `commands.events.create`

**Rola:** Kolejka komend tworzenia eventów.

**Producer:** `EventCommandController`  
**Consumer:** `EventCommandHandler` (group: `event-command-handler`)

**Format wiadomości:**
```json
{
  "eventId": "f6e9a1b2-3c4d-5e6f-7a8b-9c0d1e2f3a4b",
  "organizerId": "user-123",
  "title": "Koncert Rockowy 2025",
  "description": "Największy koncert roku z topowymi zespołami",
  "eventDate": "2025-12-31T20:00:00Z"
}
```

**Konfiguracja:**
- **Partitions:** 3 (domyślnie, dla równoległego przetwarzania)
- **Replication Factor:** 1 (dev w Redpanda), 3 (prod w Kafka)
- **Retention:** 7 dni (wiadomości są usuwane po tygodniu)

**Partycjonowanie:**
```java
kafkaTemplate.send(
    TOPIC_COMMANDS_EVENTS_CREATE, 
    command.eventId().toString(),  // ← KEY = UUID eventu
    command                         // ← VALUE = cała komenda
);
```

Redpanda (jak Kafka) używa KEY do określenia partycji:
```
hash(eventId) % numberOfPartitions = partition number
```

Dzięki temu wszystkie komendy dla tego samego eventu trafiają do tej samej partycji (ordering guarantee).

---

#### 9.1.2 Topic: `events.lifecycle`

**Rola:** Event bus dla zdarzeń domenowych (lifecycle eventów).

**Uwaga:** Nazwa została uproszczona z `domain.events.lifecycle` do `events.lifecycle`.

**Producer:** `EventCommandHandler`  
**Consumers:** 
- `EventViewProjector` (group: `eventmaster-projectors-crdb`)
- [Przyszłość] `EmailNotificationService` (group: `notifications`)
- [Przyszłość] `AuditLogService` (group: `audit`)

**Format wiadomości:**
```json
{
  "eventId": "f6e9a1b2-3c4d-5e6f-7a8b-9c0d1e2f3a4b",
  "title": "Koncert Rockowy 2025",
  "description": "Największy koncert roku",
  "eventDate": "2025-12-31T20:00:00Z",
  "organizerId": "user-123"
}
```

**Dlaczego osobny topic?**
1. **Separation of Concerns** - Komendy (intencje) ≠ Domain Events (fakty)
2. **Multiple Consumers** - Wiele serwisów może reagować na ten sam event
3. **Replay** - Możemy "odtworzyć" projekcje z domain events

---

### 9.2 Consumer Groups

**Co to jest Consumer Group?**
To grupa konsumentów pracujących razem nad tym samym topicem. Redpanda (zgodnie z API Kafki) gwarantuje, że każda wiadomość jest przetwarzana przez **dokładnie jednego** konsumenta z grupy.

```mermaid
graph TD
    A[Topic: events.lifecycle] --> B[Partition 0]
    A --> C[Partition 1]
    A --> D[Partition 2]
    
    B --> E[Consumer 1<br/>Group: projectors]
    C --> F[Consumer 2<br/>Group: projectors]
    D --> G[Consumer 3<br/>Group: projectors]
    
    B --> H[Consumer 1<br/>Group: notifications]
    C --> H
    D --> H
    
    style E fill:#51cf66
    style F fill:#51cf66
    style G fill:#51cf66
    style H fill:#ffd43b
```

**EventMaster Consumer Groups:**

| Group ID | Rola | Konsumenci |
|----------|------|------------|
| `event-command-handler` | Przetwarza komendy | EventCommandHandler (1 instancja) |
| `eventmaster-projectors-crdb` | Projektuje do Read Model | EventViewProjector (1-3 instancje) |

**Uwaga:** Nazwa grupy `eventmaster-projectors-crdb` jest historyczna (z czasów gdy planowaliśmy CockroachDB).

**Skalowanie:**
Jeśli mamy 3 partycje i 3 konsumentów w grupie:
```
Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

Jeśli dodamy 4. konsumenta → będzie idle (bez przypisanej partycji).

---

### 9.3 Offset Management

**Co to jest offset?**
Offset to pozycja (numer) wiadomości w partycji. To jak zakładka w książce - oznacza "tu skończyłem czytać".

```
Partition 0:
[0] Message A
[1] Message B
[2] Message C  ← Consumer offset = 2 (przeczytał do C)
[3] Message D
[4] Message E
```

**Jak Redpanda zarządza offsetami?**
1. Consumer przetwarza wiadomość
2. Consumer commituje offset (zapisuje do internal topicu `__consumer_offsets`)
3. Jeśli consumer padnie, nowy consumer czyta od ostatniego commitowanego offsetu

**Auto-commit vs Manual commit:**
```yaml
# application.properties
spring.kafka.consumer.enable-auto-commit=true
spring.kafka.consumer.auto-commit-interval=5000  # 5 sekund
```

EventMaster używa **auto-commit**, co jest OK dla:
- Idempotent operations (można bezpiecznie przetworzyć dwukrotnie)
- Low-risk failures

Dla mission-critical systemów, lepszy jest **manual commit** po faktycznym zapisie do DB.

---

### 9.4 Error Handling

#### 9.4.1 DefaultErrorHandler

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
    return new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(template),  // ← Co zrobić z failed message
            new FixedBackOff(1000L, 2)  // ← Czekaj 1s, maksymalnie 2 próby
    );
}
```

**Przepływ obsługi błędów:**
```mermaid
graph TD
    A[Message consumed] --> B{Processing successful?}
    B -->|YES| C[Commit offset]
    B -->|NO| D[Wait 1 second]
    D --> E{Retry count < 2?}
    E -->|YES| F[Retry processing]
    F --> B
    E -->|NO| G[Publish to DLQ]
    G --> H[Topic: *.DLT]
    
    style C fill:#51cf66
    style G fill:#ff6b6b
    style H fill:#ff6b6b
```

**DLQ Message Format:**
```json
{
  "originalTopic": "commands.events.create",
  "originalPartition": 0,
  "originalOffset": 42,
  "originalTimestamp": 1697812345678,
  "exception": "DataIntegrityViolationException: duplicate key value violates unique constraint",
  "failedAttempts": 3,
  "originalMessage": {
    "eventId": "...",
    "title": "..."
  }
}
```

---

### 9.5 Redpanda w EventMaster - podsumowanie

**Dlaczego Redpanda, a nie Apache Kafka lub RabbitMQ?**

| Cecha | Redpanda | Apache Kafka | RabbitMQ |
|-------|----------|--------------|----------|
| **Throughput** | Bardzo wysoki | Bardzo wysoki | Średni |
| **Persistence** | Tak, do dysku | Tak, do dysku | Opcjonalne |
| **Replay** | Tak (można wrócić do starego offsetu) | Tak | Nie |
| **Ordering** | Gwarantowane w partycji | Gwarantowane w partycji | Gwarantowane w kolejce |
| **Use case** | Event streaming | Event streaming, log aggregation | Task queues, RPC |
| **Setup** | Prosty (jeden proces) | Złożony (Kafka + Zookeeper) | Średni |
| **API** | Kafka-compatible | Native | AMQP |
| **Resources** | Mniej (C++) | Więcej (JVM) | Średnie |

**Dlaczego Redpanda?**
1. **Kafka-compatible** - Ten sam API, te same biblioteki klienckie (Spring Kafka działa out-of-the-box)
2. **Prostszy setup** - Nie wymaga Zookeeper, jeden proces
3. **Lżejszy** - Mniejsze zużycie pamięci i CPU (napisany w C++)
4. **Szybszy start** - Idealny do developmentu lokalnego
5. **Event-Driven** - Idealna do Domain Events i CQRS
6. **Replay** - Możemy przebudować Read Model z historii
7. **Produkcyjny** - Gotowy do produkcji (używany przez firmy jak Vectorized)

**Kiedy wybrać Apache Kafka zamiast Redpanda?**
- Bardzo duża skala (setki węzłów)
- Ekosystem narzędzi Confluent (Schema Registry, KSQL, Kafka Connect)
- Legacy system już używa Kafki

EventMaster używa Redpanda, bo:
- ✅ Łatwiejszy w setup (MVP)
- ✅ Kompatybilny z Kafka API (można przejść na Kafka bez zmian w kodzie!)
- ✅ Niższe wymagania zasobów
- ✅ Szybsz start (ważne w testach i developmencie)

---

## 10. Bezpieczeństwo i autoryzacja

### 10.1 OAuth2 i OpenID Connect (OIDC)

**Co to jest OAuth2?**
OAuth2 to protokół autoryzacji. Pozwala aplikacji A (EventMaster) uzyskać dostęp do zasobów użytkownika bez poznania jego hasła.

**Co to jest OIDC?**
OpenID Connect to rozszerzenie OAuth2 dodające autentykację (login). Daje nam:
- ID Token (informacje o użytkowniku)
- UserInfo endpoint (dodatkowe dane użytkownika)

**Aktorzy w OAuth2:**
1. **Resource Owner** - Użytkownik (właściciel danych)
2. **Client** - Nuxt Frontend (aplikacja chcąca dostępu)
3. **Authorization Server** - Keycloak (wydaje tokeny)
4. **Resource Server** - Spring Backend (chroni zasoby)

---

### 10.2 Przepływ Authorization Code Flow

```mermaid
sequenceDiagram
    participant User
    participant Nuxt as Nuxt (Client)
    participant KC as Keycloak (Auth Server)
    participant Spring as Spring (Resource Server)
    
    User->>Nuxt: 1. Klik "Zaloguj"
    Nuxt->>KC: 2. GET /auth?<br/>response_type=code&<br/>client_id=eventmaster-frontend&<br/>redirect_uri=http://localhost:3000/api/auth/callback&<br/>scope=openid profile email
    KC->>User: 3. Formularz logowania
    User->>KC: 4. POST /login (username + password)
    KC->>Nuxt: 5. Redirect /api/auth/callback?code=ABC123
    Nuxt->>KC: 6. POST /token<br/>grant_type=authorization_code&<br/>code=ABC123&<br/>client_id=...&<br/>client_secret=...
    KC->>KC: 7. Waliduje code + client credentials
    KC->>Nuxt: 8. Zwraca tokeny:<br/>{<br/>  "access_token": "eyJhbG...",<br/>  "refresh_token": "eyJhbG...",<br/>  "id_token": "eyJhbG...",<br/>  "expires_in": 3600<br/>}
    Nuxt->>Nuxt: 9. Zapisuje w session cookie (httpOnly)
    
    Note over User,Spring: Teraz użytkownik jest zalogowany
    
    User->>Nuxt: 10. Wypełnia formularz eventu
    Nuxt->>Spring: 11. POST /api/v1/events<br/>Authorization: Bearer eyJhbG...
    Spring->>KC: 12. Waliduj token (JWKS)
    KC->>Spring: 13. Public key + token valid ✅
    Spring->>Spring: 14. Dekoduje JWT, wyciąga claims
    Spring->>Nuxt: 15. 202 Accepted
```

**Dlaczego Authorization Code Flow?**
- **Bezpieczny** - Client secret nigdy nie trafia do przeglądarki
- **Refresh tokens** - Możemy odświeżyć access token bez ponownego logowania
- **Standard** - Używany przez Google, Facebook, GitHub, etc.

---

### 10.3 JWT Token Structure

**Części JWT:**
```
eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9  ← HEADER
.
eyJzdWIiOiJ1c2VyLTEyMyIsImVtYWlsI... ← PAYLOAD
.
SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJ... ← SIGNATURE
```

**Dekodowany HEADER:**
```json
{
  "alg": "RS256",         // ← Algorytm szyfrowania (RSA 256-bit)
  "typ": "JWT",           // ← Typ tokenu
  "kid": "key-id-123"     // ← ID klucza (do znalezienia public key w JWKS)
}
```

**Dekodowany PAYLOAD (claims):**
```json
{
  "sub": "f6e9a1b2-3c4d-5e6f-7a8b-9c0d1e2f3a4b",  // Subject = User ID
  "email": "jan.kowalski@example.com",
  "preferred_username": "jankowalski",
  "name": "Jan Kowalski",
  "given_name": "Jan",
  "family_name": "Kowalski",
  "iat": 1697812345,      // Issued At (timestamp)
  "exp": 1697815945,      // Expiration (iat + 1 hour)
  "iss": "http://localhost:8180/realms/eventmaster",  // Issuer
  "aud": "eventmaster-frontend",  // Audience
  "realm_access": {
    "roles": ["user"]     // Role użytkownika
  }
}
```

**SIGNATURE:**
Sygnatura jest obliczana jako:
```
RSASHA256(
  base64UrlEncode(header) + "." + base64UrlEncode(payload),
  privateKey  // ← Klucz prywatny Keycloak
)
```

Spring Backend waliduje sygnaturę używając **klucza publicznego** Keycloak (pobranego z JWKS endpoint).

---

### 10.4 JWKS (JSON Web Key Set)

**Co to jest JWKS?**
JWKS to endpoint, który zwraca klucze publiczne do walidacji JWT.

**URL:** `http://localhost:8180/realms/eventmaster/protocol/openid-connect/certs`

**Przykładowa odpowiedź:**
```json
{
  "keys": [
    {
      "kid": "key-id-123",
      "kty": "RSA",
      "alg": "RS256",
      "use": "sig",
      "n": "xGOw...==",  // ← Moduł klucza publicznego (base64)
      "e": "AQAB"       // ← Wykładnik klucza publicznego
    }
  ]
}
```

**Jak Spring używa JWKS?**
1. Odbiera JWT token z header `Authorization`
2. Dekoduje HEADER, wyciąga `kid`
3. Pobiera JWKS z Keycloak (cachowane)
4. Znajduje klucz o `kid = "key-id-123"`
5. Używa klucza publicznego do walidacji SIGNATURE
6. Jeśli OK → token valid ✅
7. Jeśli NIE → token invalid ❌ → 401 Unauthorized

---

### 10.5 SecurityConfig - szczegóły

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            // === Reguły autoryzacji ===
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/v1/events").permitAll()  
                // ↑ GET /api/v1/events - publiczne (bez tokenu)
                
                .requestMatchers(HttpMethod.POST, "/api/v1/events").authenticated()  
                // ↑ POST /api/v1/events - wymaga tokenu
                
                .requestMatchers("/actuator/health").permitAll()  
                // ↑ Health check - publiczny (dla Kubernetes liveness probe)
                
                .anyRequest().authenticated()  
                // ↑ Wszystko inne - wymaga tokenu
            )
            
            // === OAuth2 Resource Server ===
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt
                    .jwkSetUri("http://localhost:8180/realms/eventmaster/protocol/openid-connect/certs")
                    // ↑ URL do JWKS (Spring automatycznie pobiera klucze publiczne)
                )
            )
            
            // === CORS ===
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            
            // === CSRF ===
            .csrf(csrf -> csrf.disable());  
            // ↑ Wyłączamy CSRF (REST API nie używa session cookies, tylko JWT)

        return http.build();
    }

    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        
        // Dozwolone origins (skąd mogą przychodzić requesty)
        config.setAllowedOrigins(List.of("http://localhost:3000"));
        
        // Dozwolone metody HTTP
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        
        // Dozwolone headery
        config.setAllowedHeaders(List.of("*"));
        
        // Zezwól na credentials (cookies, auth headers)
        config.setAllowCredentials(true);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);  // ↑ Dla wszystkich endpointów
        return source;
    }
}
```

**Dlaczego wyłączamy CSRF?**
CSRF (Cross-Site Request Forgery) jest atakiem polegającym na:
1. Użytkownik jest zalogowany na example.com (ma session cookie)
2. Użytkownik odwiedza evil.com
3. evil.com wysyła request do example.com
4. Przeglądarka automatycznie dołącza session cookie
5. example.com myśli, że to legalny request

W przypadku JWT:
- Token **NIE** jest w cookie (albo jest httpOnly)
- Evil.com **NIE MOŻE** odczytać tokenu (Same-Origin Policy)
- Evil.com **NIE MOŻE** wysłać requestu z tokenem
- CSRF nie jest zagrożeniem ✅

---

### 10.6 Wyciąganie informacji o użytkowniku

**W kontrolerze:**
```java
@PostMapping
public ResponseEntity<Void> createEvent(
        @Valid @RequestBody CreateEventRequest request,
        @AuthenticationPrincipal Jwt jwt  // ← Spring wstrzykuje JWT
) {
    // Wyciąganie claims:
    String userId = jwt.getSubject();                    // "f6e9a1b2..."
    String email = jwt.getClaimAsString("email");        // "jan@example.com"
    String username = jwt.getClaimAsString("preferred_username");  // "jankowalski"
    List<String> roles = jwt.getClaimAsStringList("realm_access.roles");  // ["user"]
    
    // Użycie w logice:
    var command = new CreateEventCommand(
        UUID.randomUUID(),
        userId,  // ← organizerId z tokenu
        request.title(),
        request.description(),
        request.eventDate()
    );
    
    // ...
}
```

**Sprawdzanie ról:**
```java
@PreAuthorize("hasRole('ADMIN')")  // ← Tylko dla admins
@DeleteMapping("/{eventId}")
public ResponseEntity<Void> deleteEvent(@PathVariable UUID eventId) {
    // ...
}
```

---

## 11. Infrastruktura i deployment

### 11.1 Docker Compose - lokalne środowisko

**Struktura:**
```
docker/
├── docker-compose.yml
└── realm-config/
    └── eventmaster-realm.json  (opcjonalnie)
```

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  # === PostgreSQL (Write + Read Model) ===
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
      POSTGRES_DB: eventmaster_db
    volumes:
      - postgres-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d eventmaster_db"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s

  # === Keycloak (Authorization Server) ===
  keycloak:
    image: quay.io/keycloak/keycloak:latest
    environment:
      KEYCLOAK_ADMIN: admin
      KEYCLOAK_ADMIN_PASSWORD: admin
      KEYCLOAK_IMPORT: /opt/keycloak/data/import/eventmaster-realm.json
    volumes:
      - ./realm-config:/opt/keycloak/data/import
    ports:
      - "8180:8080"
    command: start-dev
    healthcheck:
      test: ["CMD-SHELL", "exec 3<>/dev/tcp/localhost/8080 && echo -e 'GET /health/ready HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3 && cat <&3 | grep -q '200 OK'"]
      interval: 15s
      timeout: 5s
      retries: 10
      start_period: 60s
    depends_on:
      postgres:
        condition: service_healthy

  # === Redpanda (Message Broker - Kafka compatible) ===
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
      - "19092:19092"  # Kafka API (external)
      - "18081:18081"  # Schema Registry
      - "18082:18082"  # Pandaproxy (REST API)
      - "9644:9644"    # Admin API
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
      interval: 15s
      timeout: 3s
      retries: 5
      start_period: 5s

  # === Caddy (Reverse Proxy) ===
  caddy:
    image: caddy:latest
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ../Caddyfile:/etc/caddy/Caddyfile
      - caddy-data:/data
    depends_on:
      - keycloak

volumes:
  postgres-data:
  caddy-data:
```

**Kluczowe zmiany względem klasycznego CQRS:**
1. **Jedna baza PostgreSQL** zamiast PostgreSQL + CockroachDB
   - Dwie tabele: `events` (Write) i `event_view` (Read)
2. **Redpanda zamiast Apache Kafka**
   - Kafka-compatible API
   - Prostszy setup (brak Zookeeper)
3. **Health checks** dla wszystkich serwisów
4. **Keycloak realm auto-import** (Infrastructure as Code)

**Uruchomienie:**
```bash
cd docker
docker-compose up -d
```

**Sprawdzanie statusu:**
```bash
docker-compose ps
docker-compose logs -f spring-backend
```

---

### 11.2 Caddyfile - routing

```
http://localhost {
    log {
        output stderr
    }

    # Reguła 1: API Backendu
    reverse_proxy /api/v1/* http://host.docker.internal:8080 {
        header_up Host {http.request.host}
    }

    # Reguła 2: API Frontendu (OAuth callback)
    reverse_proxy /api/auth/* http://host.docker.internal:3000 {
        header_up Host {http.request.host}
    }

    # Reguła 3: Wszystko inne → Nuxt
    reverse_proxy * http://host.docker.internal:3000 {
        header_up Host {http.request.host}
    }
}
```

**Dlaczego `host.docker.internal`?**
- Na Dockerze dla Mac/Windows, `host.docker.internal` wskazuje na host machine
- Spring Backend i Nuxt Frontend działają **poza** Docker Compose (na hoście)
- Tylko infrastruktura (DB, Keycloak, Caddy) w Docker

**Alternatywa - wszystko w Docker:**
```yaml
# docker-compose.yml
services:
  backend:
    build: ../
    ports:
      - "8080:8080"
    depends_on:
      - postgres
      - kafka

  frontend:
    build: ../frontend
    ports:
      - "3000:3000"
    depends_on:
      - backend
```

---

### 11.3 Uruchomienie aplikacji (Development)

**1. Start infrastruktury:**
```bash
cd docker
docker-compose up -d
```

**2. Konfiguracja Keycloak:**
```
1. Otwórz http://localhost:8180
2. Login: admin / admin
3. Utwórz realm "eventmaster"
4. Utwórz client "eventmaster-frontend":
   - Client Protocol: openid-connect
   - Access Type: confidential
   - Valid Redirect URIs: http://localhost:3000/api/auth/callback/*
   - Web Origins: http://localhost:3000
5. Utwórz użytkownika testowego
```

**3. Start backendu:**
```bash
cd ..
./mvnw spring-boot:run
```

**4. Start frontendu:**
```bash
cd frontend
npm install
npm run dev
```

**5. Otwórz aplikację:**
```
http://localhost:80  (przez Caddy)
lub
http://localhost:3000  (bezpośrednio Nuxt)
```

---

### 11.4 Deployment produkcyjny (overview)

**Architektura produkcyjna (Kubernetes):**

**Uwaga:** Diagram poniżej przedstawia klasyczne rozdzielenie CQRS z dwiema bazami. W obecnej implementacji MVP używamy jednej PostgreSQL z dwiema tabelami, co można łatwo zmienić na ten model bez zmian w kodzie aplikacji.

```mermaid
graph TB
    subgraph "Internet"
        USER[👤 Użytkownicy]
    end
    
    subgraph "Kubernetes Cluster"
        subgraph "Ingress"
            ING[Nginx Ingress]
        end
        
        subgraph "Frontend Pods"
            NUXT1[Nuxt Pod 1]
            NUXT2[Nuxt Pod 2]
            NUXT3[Nuxt Pod 3]
        end
        
        subgraph "Backend Pods"
            SPRING1[Spring Pod 1]
            SPRING2[Spring Pod 2]
            SPRING3[Spring Pod 3]
        end
        
        subgraph "Message Broker"
            RP1[Redpanda Node 1]
            RP2[Redpanda Node 2]
            RP3[Redpanda Node 3]
        end
        
        subgraph "Databases"
            PG[(PostgreSQL<br/>Write Model<br/>tabela: events)]
            PGREAD[(PostgreSQL Read Replicas<br/>Read Model<br/>tabela: event_view)]
        end
        
        subgraph "Auth"
            KC[Keycloak]
        end
    end
    
    USER --> ING
    ING --> NUXT1 & NUXT2 & NUXT3
    NUXT1 & NUXT2 & NUXT3 --> SPRING1 & SPRING2 & SPRING3
    SPRING1 & SPRING2 & SPRING3 --> RP1 & RP2 & RP3
    SPRING1 & SPRING2 & SPRING3 --> PG
    SPRING1 & SPRING2 & SPRING3 --> PGREAD
    SPRING1 & SPRING2 & SPRING3 --> KC
```

**Komponenty:**
1. **Nginx Ingress** - Routing i Load Balancing
2. **Frontend Pods** - 3 repliki Nuxt (horizontal scaling)
3. **Backend Pods** - 3 repliki Spring Boot (horizontal scaling)
4. **Redpanda Cluster** - 3 nody (high availability, Kafka-compatible)
5. **PostgreSQL Write** - Managed service (AWS RDS, Google Cloud SQL) dla Write Model
6. **PostgreSQL Read Replicas** - Read replicas dla Read Model (można też osobna baza: MongoDB, CockroachDB)
7. **Keycloak** - Clustered (2+ instances)

**Możliwe ulepszenia produkcyjne:**
- Rozdzielenie Write i Read Model na osobne fizyczne bazy (np. PostgreSQL Write + CockroachDB Read)
- Multi-region deployment dla Read Model
- Apache Kafka zamiast Redpanda dla bardzo dużej skali

**Deployment manifest (przykład - Backend):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventmaster-backend
spec:
  replicas: 3
  selector:
    matchLabels:
      app: eventmaster-backend
  template:
    metadata:
      labels:
        app: eventmaster-backend
    spec:
      containers:
      - name: backend
        image: eventmaster/backend:1.0.0
        ports:
        - containerPort: 8080
        env:
        - name: SPRING_DATASOURCE_WRITE_URL
          value: "jdbc:postgresql://postgres-write.example.com:5432/eventmaster_db"
        - name: SPRING_DATASOURCE_READ_URL
          value: "jdbc:postgresql://postgres-read.example.com:5432/eventmaster_db"  # Read replica lub osobna baza
        - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
          value: "redpanda-1:9092,redpanda-2:9092,redpanda-3:9092"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: eventmaster-backend
spec:
  selector:
    app: eventmaster-backend
  ports:
  - port: 8080
    targetPort: 8080
```

---

## 12. Słowniczek pojęć

### A
- **Agregat (Aggregate)** - Klaster encji i obiektów wartości traktowanych jako jedna jednostka. W EventMaster: `Event` jest agregatem.
- **Asynchroniczne przetwarzanie** - Wykonywanie operacji w tle, bez blokowania użytkownika.
- **Authorization Server** - Serwer wydający tokeny OAuth2 (Keycloak).

### C
- **Command** - Komenda, intencja zmiany stanu systemu (np. CreateEventCommand).
- **Consumer** - Komponent odbierający wiadomości z Kafki.
- **Consumer Group** - Grupa konsumentów pracujących razem nad tym samym topicem.
- **CORS (Cross-Origin Resource Sharing)** - Mechanizm pozwalający na requesty między różnymi domenami.
- **CQRS (Command Query Responsibility Segregation)** - Wzorzec architektoniczny rozdzielający zapisy od odczytów.

### D
- **Dead Letter Queue (DLQ)** - Kolejka dla wiadomości, które nie mogły być przetworzone.
- **Denormalizacja** - Duplikowanie danych w celu przyspieszenia odczytów.
- **Domain Event** - Zdarzenie domenowe, fakt który się wydarzył (np. EventCreatedEvent).
- **DTO (Data Transfer Object)** - Obiekt służący do przenoszenia danych między warstwami.

### E
- **Entity** - Obiekt z unikalną tożsamością, mapowany na tabelę w bazie.
- **Event Bus** - System przekazywania zdarzeń między komponentami (Kafka).
- **Event-Driven Architecture** - Architektura oparta na zdarzeniach.
- **Eventual Consistency** - Model spójności, gdzie dane mogą być chwilowo niespójne, ale ostatecznie się zsynchronizują.

### H
- **Health Check** - Endpoint sprawdzający czy aplikacja działa poprawnie.

### I
- **Idempotencja** - Właściwość operacji, która może być wykonana wielokrotnie bez zmiany wyniku.
- **Immutable** - Niezmienny obiekt (np. record w Javie).

### J
- **JWT (JSON Web Token)** - Token autentykacji w formacie JSON, podpisany cyfrowo.
- **JWKS (JSON Web Key Set)** - Zbiór kluczy publicznych do walidacji JWT.

### K
- **Kafka** - Rozproszona platforma streamingowa do przetwarzania zdarzeń (EventMaster używa Redpanda, która jest Kafka-compatible).

### M
- **Message Broker** - System pośredniczący w wymianie wiadomości (Kafka).
- **Middleware** - Kod wykonywany przed obsługą żądania (np. sprawdzanie autoryzacji).

### O
- **OAuth2** - Protokół autoryzacji.
- **Offset** - Pozycja wiadomości w partycji Kafki.
- **OIDC (OpenID Connect)** - Rozszerzenie OAuth2 dodające autentykację.

### P
- **Partition** - Podział topicu Kafki na segmenty.
- **Producer** - Komponent wysyłający wiadomości do Kafki.
- **Projector** - Komponent projektujący Domain Events do Read Model.

### Q
- **Query** - Zapytanie, żądanie odczytu danych.

### R
- **Read Model** - Model zoptymalizowany pod odczyty. W EventMaster: tabela `event_view` w PostgreSQL.
- **Redpanda** - Kafka-compatible message broker napisany w C++, lżejsza alternatywa dla Apache Kafka.
- **Record (Java)** - Immutable klasa w Javie (od Java 14).
- **Resource Server** - Serwer chroniący zasoby, walidujący tokeny (Spring Backend).
- **Retry** - Ponowna próba wykonania operacji po błędzie.
- **Reverse Proxy** - Serwer przekierowujący requesty (Caddy).

### S
- **SSR (Server-Side Rendering)** - Renderowanie HTML na serwerze (Nuxt).
- **Saga** - Wzorzec zarządzania długotrwałymi transakcjami.

### T
- **Topic** - Kategoria/kanał wiadomości w Kafce.
- **Transaction** - Grupa operacji wykonywanych atomowo.

### W
- **Write Model** - Model zoptymalizowany pod zapisy. W EventMaster: tabela `events` w PostgreSQL.

---

## Podsumowanie

EventMaster to nowoczesna aplikacja demonstrująca architekturę CQRS w praktyce. Kluczowe elementy:

1. **Separacja Read/Write** - Dwie bazy danych, dwa modele, optymalizacja pod konkretne zadania.
2. **Event-Driven** - Kafka jako centralny event bus, asynchroniczne przetwarzanie.
3. **Eventual Consistency** - Dane mogą być opóźnione, ale system jest skalowalny i wydajny.
4. **OAuth2/OIDC** - Profesjonalne zabezpieczenie z Keycloak.
5. **Modern Stack** - Java 21, Spring Boot 3, Nuxt 4, Vue 3 - najnowsze technologie.

**Dlaczego warto poznać tę architekturę?**
- Używana w największych systemach (Netflix, Uber, LinkedIn)
- Przygotowanie na mikroserwisy
- Zrozumienie event-driven architecture
- Praktyczna znajomość CQRS

**Co dalej?**
1. Implementacja dodatkowych komend (UpdateEvent, DeleteEvent)
2. Dodanie sagas dla długotrwałych procesów
3. Implementacja Event Sourcing
4. Dodanie monitoringu (Prometheus, Grafana)
5. Wdrożenie na Kubernetes

---

---

## Historia zmian

**2025-10-19** - Aktualizacja architektury:
- Zmiana z dwóch baz danych (PostgreSQL + CockroachDB) na jedną bazę PostgreSQL z dwiema tabelami (`events` i `event_view`)
- Zmiana z Apache Kafka na Redpanda (Kafka-compatible)
- Uproszczenie nazwy topicu: `events.lifecycle` (wcześniej `domain.events.lifecycle`)
- Dodanie szczegółowego opisu DataSource configuration dla Write i Read Model
- Aktualizacja wszystkich diagramów i opisów zgodnie z faktyczną implementacją

---

**Dokument stworzony:** 2025-10-19  
**Ostatnia aktualizacja:** 2025-10-19
**Liczba słów:** ~13,000  
**Autor:** Zespół EventMaster

