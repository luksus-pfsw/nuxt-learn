package com.eventmaster.backend.events;

import com.eventmaster.backend.events.command.CreateEventCommand;
import com.eventmaster.backend.events.domain.Event;
import com.eventmaster.backend.events.domain.EventCreatedEvent;
import com.eventmaster.backend.events.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventCommandHandler {

    public static final String TOPIC_COMMANDS_EVENTS_CREATE = "commands.events.create";
    public static final String TOPIC_DOMAIN_EVENTS_LIFECYCLE = "domain.events.lifecycle";

    private final EventRepository eventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate; // Generyczny template

    @KafkaListener(topics = TOPIC_COMMANDS_EVENTS_CREATE, groupId = "event-command-handler")
    @Transactional // Kluczowe: Operacja zapisu do bazy jest transakcyjna
    public void handleCreateEventCommand(CreateEventCommand command) {
        log.info("Received command to create event: {}", command.eventId());

        // 1. Mapowanie komendy na encję (Model Zapisu)
        Event event = Event.builder()
                .id(command.eventId())
                .title(command.title())
                .description(command.description())
                .eventDate(command.eventDate())
                .organizerId(command.organizerId())
                .build();

        // 2. Utrwalenie w bazie danych (Postgres)
        try {
            eventRepository.save(event);
            log.info("Event {} saved to Write Model (Postgres)", event.getId());
        } catch (Exception e) {
            // Np. DataIntegrityViolationException (jeśli UUID się powtórzy)
            log.error("Failed to save event {} to database. Error: {}", command.eventId(), e.getMessage());
            // Rzucenie wyjątku spowoduje, że Kafka spróbuje ponownie
            throw new RuntimeException("Database persistence failed, triggering retry", e);
        }

        // 3. Stworzenie Zdarzenia Domenowego
        EventCreatedEvent domainEvent = new EventCreatedEvent(
                command.eventId(),
                command.title(),
                command.description(),
                command.eventDate(),
                command.organizerId()
        );

        // 4. Publikacja Zdarzenia Domenowego na nowy topik
        kafkaTemplate.send(TOPIC_DOMAIN_EVENTS_LIFECYCLE, command.eventId().toString(), domainEvent);
        log.info("Published Domain Event {} for event {}", domainEvent.getClass().getSimpleName(), domainEvent.eventId());
    }
}
