package com.eventmaster.backend.events.query.projector;

import com.eventmaster.backend.events.domain.EventCreatedEvent;
import com.eventmaster.backend.events.query.model.EventView;
import com.eventmaster.backend.events.query.repository.EventViewRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventViewProjector {

    public static final String TOPIC_DOMAIN_EVENTS_LIFECYCLE = "domain.events.lifecycle";

    private final EventViewRepository eventViewRepository;

    @KafkaListener(topics = TOPIC_DOMAIN_EVENTS_LIFECYCLE, groupId = "eventmaster-projectors-crdb")
    public void handleEventCreated(EventCreatedEvent event) {
        log.info("Projecting EventCreatedEvent to CockroachDB: {}", event.eventId());

        EventView readModel = new EventView();
        readModel.setEventId(event.eventId());
        readModel.setTitle(event.title());
        readModel.setDescription(event.description());
        readModel.setEventDate(event.eventDate());
        readModel.setOrganizerId(event.organizerId());

        try {
            eventViewRepository.save(readModel);
            log.info("Event View {} saved to Read Model (CockroachDB)", readModel.getEventId());
        } catch (Exception e) {
            log.error("Failed to save Event View {} to CockroachDB. Error: {}", event.eventId(), e.getMessage());
            throw new RuntimeException("CockroachDB persistence failed, triggering retry", e);
        }
    }
}
