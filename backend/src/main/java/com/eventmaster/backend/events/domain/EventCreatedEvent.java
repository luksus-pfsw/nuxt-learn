package com.eventmaster.backend.events.domain;

import java.time.Instant;
import java.util.UUID;

public record EventCreatedEvent(
        UUID eventId,
        String title,
        String description,
        Instant eventDate,
        String organizerId
) {}