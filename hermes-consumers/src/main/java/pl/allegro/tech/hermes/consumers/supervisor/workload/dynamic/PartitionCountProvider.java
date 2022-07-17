package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.api.TopicName;

import java.util.Optional;

public interface PartitionCountProvider {

    Optional<Integer> provide(TopicName topicName);
}
