package dev.eventlab;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaEvents {
    private final ReportService service;
    public KafkaEvents(ReportService service) { this.service = service; }
    @Bean NewTopic requests() { return TopicBuilder.name("report-requests").partitions(1).replicas(1).build(); }
    @Bean NewTopic results() { return TopicBuilder.name("report-results").partitions(1).replicas(1).build(); }
    // Never silently skip a failed result. A malformed event will block this demo;
    // production systems normally add bounded retries and a dead-letter topic.
    @Bean DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
    @KafkaListener(topics = "report-results")
    public void onResult(String payload) throws Exception { service.complete(payload); }
}
