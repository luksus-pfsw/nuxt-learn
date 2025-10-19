# 🚀 EventMaster - Quick Start Guide

Szybki start dla projektu EventMaster (CQRS + Event-Driven Architecture)

---

## ⚡ Minimalny Start (5 minut)

### 1. Wymagania
```bash
✅ Java 21
✅ Docker + Docker Compose
✅ pnpm (dla frontendu)
```

### 2. Uruchom infrastrukturę
```bash
cd docker
docker-compose up -d
```

Poczekaj ~60s aż wszystkie serwisy będą healthy:
```bash
docker-compose ps
# STATUS wszystkich powinien być "healthy"
```

### 3. Uruchom backend
```bash
# Z głównego katalogu
./mvnw spring-boot:run
```

### 4. Uruchom frontend
```bash
cd frontend
pnpm install  # Tylko pierwszy raz
pnpm dev
```

### 5. Otwórz aplikację
```
http://localhost
```

---

## 🔑 Dostępy

| Serwis | URL | Credentials |
|--------|-----|-------------|
| **Aplikacja** | http://localhost | - |
| **Keycloak Admin** | http://localhost:8180 | admin / admin |
| **Backend API** | http://localhost:8080 | - |
| **Frontend Dev** | http://localhost:3000 | - |
| **Postgres** | localhost:5432 | user / password |
| **Redpanda Kafka** | localhost:19092 | - |

---

## 📋 Podstawowe Komendy

### Docker Compose
```bash
cd docker

# Start wszystkich serwisów
docker-compose up -d

# Status
docker-compose ps

# Logi
docker-compose logs -f

# Logi konkretnego serwisu
docker-compose logs -f redpanda
docker-compose logs -f postgres

# Stop
docker-compose down

# Stop + usunięcie volumes (UWAGA: usuwa dane!)
docker-compose down -v
```

### Backend (Maven)
```bash
# Uruchom aplikację
./mvnw spring-boot:run

# Testy
./mvnw test

# Testy integracyjne (tylko)
./mvnw test -Dtest=*IntegrationTest

# Build (bez testów)
./mvnw clean package -DskipTests

# Flyway migrations
./mvnw flyway:migrate
./mvnw flyway:info
```

### Frontend (pnpm)
```bash
cd frontend

# Install dependencies
pnpm install

# Development server
pnpm dev

# Build
pnpm build

# Preview production build
pnpm preview

# Lint
pnpm lint
```

### Redpanda (Kafka)
```bash
# Wejdź do kontenera
docker exec -it docker-redpanda-1 /bin/bash

# Lista topików
rpk topic list

# Szczegóły topiku
rpk topic describe commands.events.create

# Konsumuj wiadomości (od początku)
rpk topic consume commands.events.create --offset start

# Konsumuj wiadomości (bieżące)
rpk topic consume events.lifecycle

# Utwórz topic ręcznie
rpk topic create test-topic

# Health check
rpk cluster health
```

---

## 🧪 Testowanie Przepływu

### 1. Utwórz event (Command Path)

**Przez UI:**
1. Otwórz: http://localhost
2. Kliknij "Zaloguj się" (redirect do Keycloak)
3. Zaloguj się (musisz utworzyć użytkownika w Keycloak)
4. Przejdź do: http://localhost/events/create
5. Wypełnij formularz i wyślij

**Przez API (z tokenem):**
```bash
# Najpierw uzyskaj token z Keycloak (pomiń jeśli masz)
TOKEN="your-jwt-token"

# Wyślij komendę
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "title": "Test Event",
    "description": "Test description",
    "eventDate": "2025-12-31T20:00:00Z"
  }'

# Odpowiedź: 202 Accepted
```

### 2. Sprawdź Write Model (PostgreSQL)
```bash
docker exec -it docker-postgres-1 psql -U user -d eventmaster_db

# W psql:
SELECT * FROM events;
```

### 3. Sprawdź Kafka Topics
```bash
docker exec -it docker-redpanda-1 rpk topic consume commands.events.create --offset start
# Ctrl+C aby wyjść

docker exec -it docker-redpanda-1 rpk topic consume events.lifecycle --offset start
```

### 4. Sprawdź Read Model (PostgreSQL)
```bash
docker exec -it docker-postgres-1 psql -U user -d eventmaster_db

# W psql:
SELECT * FROM event_view;
```

### 5. Pobierz listę (Query Path)
```bash
# TODO: EventQueryController jeszcze nie zwraca danych
curl http://localhost:8080/api/v1/events
```

---

## 🐛 Troubleshooting

### Backend nie startuje - błąd połączenia z Postgres
```bash
# Sprawdź czy Postgres działa
docker-compose ps postgres

# Sprawdź logi
docker-compose logs postgres

# Sprawdź czy port 5432 jest wolny
lsof -i :5432

# Restart Postgres
docker-compose restart postgres
```

### Backend nie startuje - błąd połączenia z Redpanda
```bash
# Sprawdź czy Redpanda działa
docker-compose ps redpanda

# Sprawdź health
docker exec -it docker-redpanda-1 rpk cluster health

# Sprawdź logi
docker-compose logs redpanda

# Restart Redpanda
docker-compose restart redpanda
```

### Keycloak nie importuje realm
```bash
# Sprawdź czy plik istnieje w kontenerze
docker exec -it docker-keycloak-1 ls -la /opt/keycloak/data/import/

# Powinien być: eventmaster-realm.json

# Sprawdź logi importu
docker-compose logs keycloak | grep -i import

# Jeśli nie ma - sprawdź volume mount
cat docker/docker-compose.yml | grep -A 3 keycloak:
```

### Frontend - błąd 401 przy próbie utworzenia eventu
```bash
# Sprawdź czy jesteś zalogowany
# W przeglądarce: DevTools → Application → Cookies
# Powinien być: authjs.session-token

# Sprawdź czy backend waliduje token
docker-compose logs backend | grep -i jwt

# Sprawdź issuer-uri
cat src/main/resources/application.properties | grep issuer-uri
```

### Docker Compose - serwisy nie są healthy
```bash
# Sprawdź szczegóły health check
docker inspect docker-postgres-1 | grep -A 20 Health

# Ręcznie sprawdź health
docker exec docker-postgres-1 pg_isready -U user
docker exec docker-redpanda-1 rpk cluster health

# Zwiększ start_period jeśli Keycloak za wolno startuje
# W docker-compose.yml dla keycloak:
#   healthcheck:
#     start_period: 90s  # było 60s
```

---

## 📊 Monitorowanie

### Sprawdź status aplikacji
```bash
# Backend health
curl http://localhost:8080/actuator/health

# Backend info
curl http://localhost:8080/actuator/info

# Keycloak health
curl http://localhost:8180/health/ready

# Redpanda cluster
docker exec -it docker-redpanda-1 rpk cluster health
```

### Logi w czasie rzeczywistym
```bash
# Wszystkie serwisy
docker-compose logs -f

# Tylko backend (gdy uruchomiony przez mvnw)
tail -f nohup.out  # jeśli uruchamiasz w tle

# Tylko infrastruktura
docker-compose logs -f postgres keycloak redpanda caddy
```

---

## 🔧 Development Tips

### Hot Reload

**Backend:**
```bash
# Spring Boot DevTools jest włączony
# Zmiany w kodzie automatycznie trigggerują restart
./mvnw spring-boot:run
```

**Frontend:**
```bash
# Nuxt Hot Module Replacement
pnpm dev
# Zmiany w .vue są natychmiastowe
```

### Debug Backend
```bash
# Uruchom z debug portem
./mvnw spring-boot:run -Dspring-boot.run.jvmArguments="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

# W IntelliJ IDEA:
# Run → Edit Configurations → + → Remote JVM Debug
# Port: 5005
```

### Reset Everything
```bash
# Stop wszystko
docker-compose down -v

# Usuń target (backend)
rm -rf target/

# Usuń node_modules (frontend)
rm -rf frontend/node_modules/
rm -rf frontend/.nuxt/

# Start od nowa
docker-compose up -d
./mvnw clean install
cd frontend && pnpm install
```

---

## 📚 Kolejne Kroki

Po udanym uruchomieniu:

1. **Przeczytaj dokumentację:**
   - README.md - Pełny opis projektu
   - ARCHITEKTURA_SZCZEGOLOWA.md - Szczegółowa architektura
   - ARCHITECTURE_REVIEW.md - Code review

2. **Eksploruj kod:**
   - src/main/java/com/eventmaster/backend/events/ - Command + Query side
   - frontend/pages/events/ - Strony Nuxt

3. **Zrób TODO:**
   - Podłącz EventQueryController do EventViewRepository
   - Wyświetl listę eventów na /events

4. **Dodaj funkcjonalności:**
   - UPDATE event
   - DELETE event
   - Szczegóły eventu

---

**Powodzenia!** 🚀

W razie problemów zajrzyj do:
- PODSUMOWANIE_ZMIAN.md
- Troubleshooting w README.md
