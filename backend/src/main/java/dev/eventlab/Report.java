package dev.eventlab;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reports")
public class Report {
    @Id public UUID id;
    @Column(nullable = false, length = 5000) public String text;
    @Column(nullable = false) public String status;
    public Integer wordCount;
    public Integer characterCount;
    public Instant createdAt;
    public Instant completedAt;

    protected Report() {} // Hibernate needs a no-argument constructor.
    public Report(String text) {
        this.id = UUID.randomUUID();
        this.text = text;
        this.status = "QUEUED";
        this.createdAt = Instant.now();
    }
}
