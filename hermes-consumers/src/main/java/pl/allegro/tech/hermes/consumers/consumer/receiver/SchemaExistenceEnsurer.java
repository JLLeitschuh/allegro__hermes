package pl.allegro.tech.hermes.consumers.consumer.receiver;

import java.time.Duration;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.message.wrapper.SchemaOnlineChecksRateLimiter;
import pl.allegro.tech.hermes.schema.SchemaException;
import pl.allegro.tech.hermes.schema.SchemaId;
import pl.allegro.tech.hermes.schema.SchemaRepository;
import pl.allegro.tech.hermes.schema.SchemaVersion;

import static java.lang.String.format;


public class SchemaExistenceEnsurer {
    private final SchemaRepository schemaRepository;
    private final SchemaOnlineChecksRateLimiter rateLimiter;

    public SchemaExistenceEnsurer(SchemaRepository schemaRepository, Duration waitSchemaInterval,
                                  SchemaOnlineChecksRateLimiter rateLimiter) {
        this.schemaRepository = schemaRepository;
        this.rateLimiter = rateLimiter;
    }

    public void ensureSchemaExists(Topic topic, SchemaVersion version) {
        pullSchemaIfNeeded(topic, version);
    }

    public void ensureSchemaExists(Topic topic, SchemaId id) {
        pullSchemaIfNeeded(topic, id);
    }

    private void pullSchemaIfNeeded(Topic topic, SchemaVersion version) {
        if (!rateLimiter.tryAcquireOnlineCheckPermit()) {
            throw new SchemaNotLoaded("Too many requests to schema-registry...");
        }
        try {
            schemaRepository.getAvroSchema(topic, version);
        } catch (SchemaException ex) {
            pullVersionsOnline(topic);
            throw new SchemaNotLoaded(format("Could not find schema version [%s] provided in header for topic [%s]." +
                    " Trying pulling online...", version, topic), ex);
        }
    }

    private void pullSchemaIfNeeded(Topic topic, SchemaId id) {
        if (!rateLimiter.tryAcquireOnlineCheckPermit()) {
            throw new SchemaNotLoaded("Too many requests to schema-registry...");
        }
        try {
            schemaRepository.getAvroSchema(topic, id);
        } catch (SchemaException ex) {
            throw new SchemaNotLoaded(format("Could not find schema id [%s] provided in header for topic [%s]." +
                    " Trying pulling online...", id, topic), ex);
        }
    }

    private void pullVersionsOnline(Topic topic) {
        schemaRepository.refreshVersions(topic);
    }

    public static class SchemaNotLoaded extends RuntimeException {
        SchemaNotLoaded(String msg, Throwable th) {
            super(msg, th);
        }

        SchemaNotLoaded(String msg) {
            super(msg);
        }
    }
}
