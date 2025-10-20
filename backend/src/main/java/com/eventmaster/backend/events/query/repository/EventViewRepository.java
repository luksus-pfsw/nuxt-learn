package com.eventmaster.backend.events.query.repository;

import com.eventmaster.backend.events.query.model.EventView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface EventViewRepository extends JpaRepository<EventView, UUID> {
}
