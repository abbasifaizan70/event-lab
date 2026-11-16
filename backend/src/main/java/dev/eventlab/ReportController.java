package dev.eventlab;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/reports")
public class ReportController {
    private final ReportRepository reports;
    private final ReportService service;
    private final BrowserEvents browser;
    public ReportController(ReportRepository reports, ReportService service, BrowserEvents browser) {
        this.reports = reports; this.service = service; this.browser = browser;
    }
    public record Request(@NotBlank @Size(max = 5000) String text) {}
    @GetMapping public List<Report> list() { return reports.findAllByOrderByCreatedAtDesc(); }
    @PostMapping @ResponseStatus(HttpStatus.ACCEPTED)
    public Report request(@Valid @RequestBody Request request) {
        try { return service.request(request.text()); }
        catch (Exception error) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Request could not be confirmed. A queued row may already exist; check the list and backend logs.", error);
        }
    }
    @GetMapping(value = "/events", produces = "text/event-stream")
    public SseEmitter events() { return browser.subscribe(); }
}
