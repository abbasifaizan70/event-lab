package dev.eventlab;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class ReportService {
    private static final Logger log = LoggerFactory.getLogger(ReportService.class);
    private final ReportRepository reports;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final BrowserEvents browser;
    private final TransactionTemplate transaction;

    public ReportService(ReportRepository reports, KafkaTemplate<String, String> kafka,
                         ObjectMapper json, BrowserEvents browser, TransactionTemplate transaction) {
        this.reports = reports; this.kafka = kafka; this.json = json;
        this.browser = browser; this.transaction = transaction;
    }
    public Report request(String text) throws Exception {
        // Repository save commits before publishing, so a fast result can find this row.
        var report = reports.save(new Report(text));
        var event = Map.of("type", "ReportRequested", "version", 1,
                "reportId", report.id.toString(), "text", report.text);
        // This waits for Kafka's acknowledgement, NOT for Django's work.
        // DB + Kafka is a dual write: see README's reliability exercise.
        kafka.send("report-requests", report.id.toString(), json.writeValueAsString(event))
                .get(12, TimeUnit.SECONDS);
        log.info("Published ReportRequested reportId={}", report.id);
        browser.changed(report.id.toString());
        return report;
    }
    public void complete(String payload) throws Exception {
        var event = json.readTree(payload);
        if (!"ReportCompleted".equals(event.path("type").asText()) || event.path("version").asInt() != 1)
            throw new IllegalArgumentException("Unsupported result event");
        var id = UUID.fromString(event.required("reportId").asText());
        if (!event.path("wordCount").canConvertToInt() || !event.path("characterCount").canConvertToInt())
            throw new IllegalArgumentException("Missing report counts");
        // Commit the result BEFORE telling the browser to refetch it.
        transaction.executeWithoutResult(status -> {
            var report = reports.findById(id).orElseThrow();
            if ("COMPLETED".equals(report.status)) return; // Safe duplicate delivery.
            report.status = "COMPLETED";
            report.wordCount = event.get("wordCount").asInt();
            report.characterCount = event.get("characterCount").asInt();
            report.completedAt = Instant.now();
            reports.save(report);
        });
        log.info("Saved ReportCompleted reportId={}", id);
        browser.changed(id.toString());
    }
}
