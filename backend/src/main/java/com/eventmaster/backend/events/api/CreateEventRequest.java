package com.eventmaster.backend.events.api;

import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

public record CreateEventRequest(
    @NotBlank(message = "Tytuł nie może być pusty") String title,
    String description,
    @Future(message = "Data musi być w przyszłości") Instant eventDate
) {}
