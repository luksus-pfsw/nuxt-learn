package com.eventmaster.backend.events.query.api;

import com.eventmaster.backend.events.query.dto.EventViewDTO;
import com.eventmaster.backend.events.query.repository.EventViewRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventQueryController {

    private final EventViewRepository eventViewRepository;

    @GetMapping
    public ResponseEntity<List<EventViewDTO>> getAllEvents() {
        List<EventViewDTO> events = eventViewRepository.findAll()
                .stream()
                .map(eventView -> new EventViewDTO(
                        eventView.getEventId(),
                        eventView.getTitle(),
                        eventView.getEventDate()
                ))
                .collect(Collectors.toList());

        return ResponseEntity.ok(events);
    }
}
