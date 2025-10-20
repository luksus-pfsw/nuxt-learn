package com.eventmaster.backend.events.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

public record CreateEventCommand(
    @NotNull UUID eventId,
    @NotNull String organizerId,
    @NotBlank String title,
    String description,
    @NotNull Instant eventDate
) {}
