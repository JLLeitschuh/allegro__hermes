package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionLoad that = (SubscriptionLoad) o;
        return loadStatus == that.loadStatus
                && Objects.equals(throughput, that.throughput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadStatus, throughput);
    }
}
