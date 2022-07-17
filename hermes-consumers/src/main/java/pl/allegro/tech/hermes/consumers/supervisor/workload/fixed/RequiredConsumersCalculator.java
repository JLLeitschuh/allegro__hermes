package pl.allegro.tech.hermes.consumers.supervisor.workload.fixed;

import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkloadConstraints;

import java.util.Optional;

class RequiredConsumersCalculator {

    private final int consumersPerSubscription;
    private final WorkloadConstraints workloadConstraints;
    private final int activeConsumerCount;

    RequiredConsumersCalculator(int consumersPerSubscription,
                                WorkloadConstraints workloadConstraints,
                                int activeConsumerCount) {
        this.consumersPerSubscription = consumersPerSubscription;
        this.workloadConstraints = workloadConstraints;
        this.activeConsumerCount = activeConsumerCount;
    }

    int calculate(SubscriptionName subscriptionName) {
        Optional<Integer> overwrittenConsumerCount = workloadConstraints.getOverwrittenConsumerCount(subscriptionName);
        return overwrittenConsumerCount
                .map(integer -> Math.min(activeConsumerCount, integer))
                .orElse(consumersPerSubscription);
    }
}
