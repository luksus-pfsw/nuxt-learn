# 🎓 LEKCJA 51: PostgreSQL 18 Testing - WARSZTAT PRAKTYCZNY

**Czas trwania:** 1 tydzień (5 dni roboczych)  
**Format:** Krok po kroku - Architekt implementuje → Tester pisze testy  
**Cel:** Opanowanie testowania PostgreSQL 18 z Testcontainers

---

## 📋 PLAN WARSZTATU (5 Dni)

### Dzień 1: Setup + MERGE Statement
- **Rano:** Setup Testcontainers PostgreSQL 18
- **Popołudnie:** Implementacja MERGE + testy idempotentności

### Dzień 2: Optimistic Locking
- **Rano:** Implementacja @Version w JPA
- **Popołudnie:** Testy concurrent modifications

### Dzień 3: Query Performance
- **Rano:** Slow query + indeksy
- **Popołudnie:** EXPLAIN ANALYZE + optymalizacja

### Dzień 4: Flyway Migrations
- **Rano:** Migration Up + Down
- **Popołudnie:** Rollback testing + schema consistency

### Dzień 5: Connection Pool
- **Rano:** HikariCP exhaustion scenario
- **Popołudnie:** Load testing + recovery

---

## 🚀 DZIEŃ 1: MERGE STATEMENT + TESTCONTAINERS

### Krok 1.1: Setup Testcontainers (ARCHITEKT)

**Zadanie:** Dodaj Testcontainers PostgreSQL 18 do projektu

**Backend:** `backend/src/test/java/com/eventmaster/backend/BaseIntegrationTest.java`

```java
package com.eventmaster.backend;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
public abstract class BaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }
}
```

**✅ CHECKPOINT (TESTER):**
```bash
# Terminal
cd backend
./mvnw test -Dtest=BaseIntegrationTest

# Oczekiwany output:
# [INFO] Tests run: 0, Failures: 0, Errors: 0, Skipped: 0
# [INFO] Testcontainers started PostgreSQL 18 on port xxxxx
```

---

### Krok 1.2: Implementacja Event Entity z MERGE (ARCHITEKT)

**Backend:** `backend/src/main/java/com/eventmaster/backend/events/domain/Event.java`

```java
package com.eventmaster.backend.events.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "events")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private LocalDate eventDate;

    @Column
    private String location;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status = EventStatus.DRAFT;

    @Version
    private Long version;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column
    private LocalDateTime updatedAt;

    @PreUpdate
    void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}
```

**Backend:** `backend/src/main/java/com/eventmaster/backend/events/domain/EventStatus.java`

```java
package com.eventmaster.backend.events.domain;

public enum EventStatus {
    DRAFT,
    PUBLISHED,
    CANCELLED
}
```

---

### Krok 1.3: Repository z MERGE (ARCHITEKT)

**Backend:** `backend/src/main/java/com/eventmaster/backend/events/repository/EventRepository.java`

```java
package com.eventmaster.backend.events.repository;

import com.eventmaster.backend.events.domain.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.UUID;

@Repository
public interface EventRepository extends JpaRepository<Event, UUID> {

    /**
     * PostgreSQL 18 MERGE Statement (Upsert)
     * 
     * Syntax: MERGE INTO target USING source ON condition
     *         WHEN MATCHED THEN UPDATE
     *         WHEN NOT MATCHED THEN INSERT
     */
    @Modifying
    @Query(value = """
        MERGE INTO events AS target
        USING (VALUES (
            CAST(:id AS UUID),
            :name,
            CAST(:eventDate AS DATE),
            :location,
            CAST(:status AS VARCHAR)
        )) AS source (id, name, event_date, location, status)
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET 
                name = source.name,
                event_date = source.event_date,
                location = source.location,
                status = CAST(source.status AS event_status),
                updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (id, name, event_date, location, status, created_at, version)
            VALUES (source.id, source.name, source.event_date, source.location, 
                    CAST(source.status AS event_status), CURRENT_TIMESTAMP, 0)
        """, nativeQuery = true)
    void upsertEvent(
        @Param("id") UUID id,
        @Param("name") String name,
        @Param("eventDate") LocalDate eventDate,
        @Param("location") String location,
        @Param("status") String status
    );
}
```

**Backend:** `backend/src/main/resources/db/migration/V1__Create_Events_Table.sql`

```sql
-- Flyway Migration V1
CREATE TYPE event_status AS ENUM ('DRAFT', 'PUBLISHED', 'CANCELLED');

CREATE TABLE events (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    event_date DATE NOT NULL,
    location VARCHAR(255),
    status event_status NOT NULL DEFAULT 'DRAFT',
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_events_event_date ON events(event_date);
CREATE INDEX idx_events_status ON events(status);
```

**✅ CHECKPOINT (ARCHITEKT):**
```bash
# Uruchom aplikację - Flyway zastosuje migrację
./mvnw spring-boot:run

# Sprawdź logi:
# Flyway: Migrating schema "public" to version "1 - Create Events Table"
```

---

### Krok 1.4: TESTY - MERGE Idempotency (TESTER - TY!)

**Twoje zadanie:** Napisz testy dla MERGE statement

**Backend:** `backend/src/test/java/com/eventmaster/backend/events/MergeStatementTest.java`

```java
package com.eventmaster.backend.events;

import com.eventmaster.backend.BaseIntegrationTest;
import com.eventmaster.backend.events.domain.Event;
import com.eventmaster.backend.events.domain.EventStatus;
import com.eventmaster.backend.events.repository.EventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MergeStatementTest extends BaseIntegrationTest {

    @Autowired
    private EventRepository eventRepository;

    @Test
    @Transactional
    void shouldInsertNewEventUsingMerge() {
        // TODO (TESTER): Napisz test
        // Given: Nowy event (nie istnieje w bazie)
        UUID eventId = UUID.randomUUID();
        String name = "Spring Conference 2025";
        LocalDate eventDate = LocalDate.of(2025, 6, 15);
        String location = "Warsaw";
        
        // When: Wywołaj upsertEvent (MERGE)
        eventRepository.upsertEvent(
            eventId, 
            name, 
            eventDate, 
            location, 
            EventStatus.DRAFT.name()
        );
        eventRepository.flush();
        
        // Then: Event został zapisany
        Event savedEvent = eventRepository.findById(eventId).orElseThrow();
        
        // TODO: Dodaj asercje
        assertThat(savedEvent.getName()).isEqualTo(name);
        assertThat(savedEvent.getEventDate()).isEqualTo(eventDate);
        assertThat(savedEvent.getLocation()).isEqualTo(location);
        assertThat(savedEvent.getStatus()).isEqualTo(EventStatus.DRAFT);
        assertThat(savedEvent.getVersion()).isEqualTo(0L); // Nowy rekord
    }

    @Test
    @Transactional
    void shouldUpdateExistingEventUsingMerge() {
        // TODO (TESTER): Napisz test
        // Given: Event już istnieje w bazie
        UUID eventId = UUID.randomUUID();
        Event existingEvent = new Event();
        existingEvent.setId(eventId);
        existingEvent.setName("Old Name");
        existingEvent.setEventDate(LocalDate.of(2025, 1, 1));
        existingEvent.setLocation("Krakow");
        existingEvent.setStatus(EventStatus.DRAFT);
        eventRepository.saveAndFlush(existingEvent);
        
        // When: Wywołaj upsertEvent z nowymi danymi (MERGE UPDATE)
        String newName = "Updated Name";
        String newLocation = "Warsaw";
        eventRepository.upsertEvent(
            eventId,
            newName,
            LocalDate.of(2025, 6, 15),
            newLocation,
            EventStatus.PUBLISHED.name()
        );
        eventRepository.flush();
        eventRepository.clear(); // Clear cache
        
        // Then: Event został zaktualizowany
        Event updatedEvent = eventRepository.findById(eventId).orElseThrow();
        
        // TODO: Dodaj asercje
        assertThat(updatedEvent.getName()).isEqualTo(newName);
        assertThat(updatedEvent.getLocation()).isEqualTo(newLocation);
        assertThat(updatedEvent.getStatus()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(updatedEvent.getUpdatedAt()).isNotNull(); // @PreUpdate
    }

    @Test
    @Transactional
    void shouldBeIdempotent_WhenCalledMultipleTimes() {
        // TODO (TESTER): Test idempotentności
        // Given: Event data
        UUID eventId = UUID.randomUUID();
        String name = "Conference";
        LocalDate eventDate = LocalDate.of(2025, 6, 15);
        String location = "Warsaw";
        
        // When: Wywołaj MERGE 5 razy z tymi samymi danymi
        for (int i = 0; i < 5; i++) {
            eventRepository.upsertEvent(
                eventId,
                name,
                eventDate,
                location,
                EventStatus.DRAFT.name()
            );
            eventRepository.flush();
        }
        
        // Then: W bazie jest tylko 1 rekord
        long count = eventRepository.count();
        assertThat(count).isEqualTo(1);
        
        // And: Event ma poprawne dane
        Event event = eventRepository.findById(eventId).orElseThrow();
        assertThat(event.getName()).isEqualTo(name);
    }

    @Test
    @Transactional
    void shouldHandleConcurrentMerges() {
        // TODO (TESTER): Test współbieżności (Advanced)
        // Given: 2 wątki próbują zmergować ten sam event
        UUID eventId = UUID.randomUUID();
        
        // When: Concurrent upserts
        // (Testcontainers + @Transactional - symulacja)
        
        // Then: Brak deadlocków, jeden z upsertów wygrywa
        // (To będziemy testować w Dniu 2 - Optimistic Locking)
    }
}
```

**🏃 URUCHOM TESTY:**
```bash
cd backend
./mvnw test -Dtest=MergeStatementTest

# Oczekiwany output:
# [INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

---

### ✅ CHECKPOINT DZIEŃ 1 (TESTER):

**Pytania kontrolne:**
1. ❓ Co to jest MERGE statement? → Upsert (INSERT lub UPDATE w jednej operacji)
2. ❓ Dlaczego używamy MERGE zamiast `saveAndFlush()`? → Atomowość, wydajność
3. ❓ Co to jest idempotentność? → Wielokrotne wywołanie = ten sam efekt
4. ❓ Jak Testcontainers uruchamia PostgreSQL 18? → Docker container

**Zadania domowe (opcjonalnie):**
- [ ] Dodaj test dla NULL values w MERGE
- [ ] Dodaj test dla niepoprawnego UUID
- [ ] Zmierz czas wykonania MERGE vs `saveAndFlush()` (100 operacji)

---

## 📋 CO DALEJ?

### Dzień 2: Optimistic Locking
**Dokument:** `CZESC_VI_LEKCJA_51_DZIEN_2_WARSZTAT.md` (zostanie utworzony)

### Dzień 3: Query Performance
**Dokument:** `CZESC_VI_LEKCJA_51_DZIEN_3_WARSZTAT.md`

### Dzień 4: Flyway Migrations
**Dokument:** `CZESC_VI_LEKCJA_51_DZIEN_4_WARSZTAT.md`

### Dzień 5: Connection Pool
**Dokument:** `CZESC_VI_LEKCJA_51_DZIEN_5_WARSZTAT.md`

---

## 🎯 NASTĘPNY KROK

**Po ukończeniu Dnia 1:**
1. ✅ Uruchom wszystkie testy (`./mvnw test`)
2. ✅ Commit + Push do Git
3. ✅ Przejdź do **Dzień 2: Optimistic Locking**

**Jeśli masz pytania:**
- Sprawdź sekcję FAQ w `CZESC_VI_TESTOWANIE_PRAKTYCZNE_STOSU.md`
- GitHub Issues (tag: question)

---

**Status:** ✅ DZIEŃ 1 GOTOWY  
**Następny:** Dzień 2 - Optimistic Locking (Concurrent Modifications)  
**Format:** Architekt implementuje → Tester pisze testy (TY!)

🎓 **Powodzenia! Let's test PostgreSQL 18 like a pro! 🚀**
