package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;

import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

public class WorkloadConstraints {

    private final Map<SubscriptionName, Constraints> subscriptionConstraints;
    private final Map<TopicName, Constraints> topicConstraints;

    public WorkloadConstraints(Map<SubscriptionName, Constraints> subscriptionConstraints,
                               Map<TopicName, Constraints> topicConstraints) {
        this.subscriptionConstraints = subscriptionConstraints != null ? subscriptionConstraints : emptyMap();
        this.topicConstraints = topicConstraints != null ? topicConstraints : emptyMap();
    }

    public Optional<Integer> getOverwrittenConsumerCount(SubscriptionName subscriptionName) {
        Constraints requiredConsumers = subscriptionConstraints.get(subscriptionName);
        if (requiredConsumers == null) {
            requiredConsumers = topicConstraints.get(subscriptionName.getTopicName());
        }
        if (requiredConsumers != null && requiredConsumers.getConsumersNumber() > 0) {
            return Optional.of(requiredConsumers.getConsumersNumber());
        }
        return Optional.empty();
    }
}
