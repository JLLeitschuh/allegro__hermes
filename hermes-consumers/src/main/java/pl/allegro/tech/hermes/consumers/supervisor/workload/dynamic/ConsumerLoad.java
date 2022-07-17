package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class ConsumerLoad {
    private final String consumerId;
    private final Map<SubscriptionName, Throughput> assignedTasks = new HashMap<>();

    ConsumerLoad(String consumerId) {
        this.consumerId = consumerId;
    }

    void assign(SubscriptionName subscriptionName, Throughput throughput) {
        assignedTasks.put(subscriptionName, throughput);
    }

    String getConsumerId() {
        return consumerId;
    }

    Set<SubscriptionName> getSubscriptions() {
        return assignedTasks.keySet();
    }

    Throughput throughput() {
        return assignedTasks.values().stream()
                .reduce(Throughput.ZERO, Throughput::add);
    }

    Map<SubscriptionName, Throughput> tasks() {
        return assignedTasks;
    }

    void removeAllTasks() {
        assignedTasks.clear();
    }

    boolean isAssigned(SubscriptionName task) {
        return assignedTasks.containsKey(task);
    }

    int getTaskCount() {
        return assignedTasks.size();
    }
}
