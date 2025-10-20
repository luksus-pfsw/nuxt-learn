package com.eventmaster.backend.events.api;

import com.eventmaster.backend.events.command.CreateEventCommand;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventCommandController {

    private final KafkaTemplate<String, CreateEventCommand> kafkaTemplate;
    public static final String TOPIC_COMMANDS_EVENTS_CREATE = "commands.events.create";

    @PostMapping
    public ResponseEntity<Void> createEvent(
            @Valid @RequestBody CreateEventRequest request,
            @AuthenticationPrincipal Jwt jwt) {

        String organizerId = jwt.getSubject();

        var command = new CreateEventCommand(
            UUID.randomUUID(),
            organizerId,
            request.title(),
            request.description(),
            request.eventDate()
        );

        kafkaTemplate.send(TOPIC_COMMANDS_EVENTS_CREATE, command.eventId().toString(), command);

        return ResponseEntity.accepted().build();
    }
}
