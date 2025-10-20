package com.eventmaster.backend.events.query.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "event_views")
@Data
public class EventView {
    @Id
    private UUID eventId;
    private String title;
    private String description;
    private Instant eventDate;
    private String organizerId;
}
