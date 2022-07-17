package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Map;

class WorkloadMetricsSnapshot {

    static final WorkloadMetricsSnapshot UNDEFINED = new WorkloadMetricsSnapshot(-1, Map.of());

    private final double cpuUtilization;
    private final Map<SubscriptionName, SubscriptionLoad> subscriptionLoads;

    WorkloadMetricsSnapshot(double cpuUtilization, Map<SubscriptionName, SubscriptionLoad> subscriptionLoads) {
        this.cpuUtilization = cpuUtilization;
        this.subscriptionLoads = subscriptionLoads;
    }

    double getCpuUtilization() {
        return cpuUtilization;
    }

    Map<SubscriptionName, SubscriptionLoad> getLoads() {
        return subscriptionLoads;
    }

    boolean isDefined() {
        return cpuUtilization != UNDEFINED.cpuUtilization;
    }
}
