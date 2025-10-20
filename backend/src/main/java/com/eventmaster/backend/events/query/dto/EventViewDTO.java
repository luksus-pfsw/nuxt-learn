package com.eventmaster.backend.events.query.dto;

import java.time.Instant;
import java.util.UUID;

public record EventViewDTO(
        UUID eventId,
        String title,
        Instant eventDate
) {}
