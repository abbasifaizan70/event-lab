package dev.eventlab;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class BrowserEvents {
    private final Set<SseEmitter> clients = ConcurrentHashMap.newKeySet();

    public SseEmitter subscribe() {
        var emitter = new SseEmitter(0L);
        clients.add(emitter);
        emitter.onCompletion(() -> clients.remove(emitter));
        emitter.onTimeout(() -> clients.remove(emitter));
        emitter.onError(error -> clients.remove(emitter));
        send(emitter, "ready", "connected");
        return emitter;
    }
    public void changed(String id) { clients.forEach(client -> send(client, "report-updated", id)); }
    @Scheduled(fixedRate = 15000)
    public void heartbeat() { clients.forEach(client -> send(client, "heartbeat", "alive")); }
    private void send(SseEmitter client, String event, String value) {
        try { client.send(SseEmitter.event().name(event).data(value)); }
        catch (Exception error) { clients.remove(client); client.complete(); }
    }
}
