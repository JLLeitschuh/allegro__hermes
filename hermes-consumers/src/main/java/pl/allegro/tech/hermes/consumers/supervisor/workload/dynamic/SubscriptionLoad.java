package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;

class SubscriptionLoad {

    private final LoadStatus loadStatus;
    private final Throughput throughput;

    SubscriptionLoad(LoadStatus loadStatus, Throughput throughput) {
        this.loadStatus = loadStatus;
        this.throughput = throughput;
    }

    LoadStatus getLoadStatus() {
        return loadStatus;
    }

    Throughput getThroughput() {
        return throughput;
    }
}
