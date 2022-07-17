package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionId;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.MessageHeaderEncoder;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.SubscriptionLoadStatus;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.ThroughputEncoder;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.WorkloadMetricsEncoder;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.WorkloadMetricsEncoder.SubscriptionLoadsEncoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.SubscriptionLoadStatus.NORMAL;
import static pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.SubscriptionLoadStatus.OVERLOADED;
import static pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.SubscriptionLoadStatus.UNDERLOADED;

class ConsumerWorkloadMetricsEncoder {

    private final SubscriptionIds subscriptionIds;
    private final MutableDirectBuffer buffer;

    ConsumerWorkloadMetricsEncoder(SubscriptionIds subscriptionIds, int bufferSize) {
        this.subscriptionIds = subscriptionIds;
        this.buffer = new ExpandableDirectByteBuffer(bufferSize);
    }

    byte[] encode(WorkloadMetricsSnapshot metrics) {
        Map<SubscriptionId, SubscriptionLoad> subscriptionLoads = mapToSubscriptionIds(metrics);

        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        WorkloadMetricsEncoder body = new WorkloadMetricsEncoder()
                .wrapAndApplyHeader(buffer, 0, headerEncoder);

        SubscriptionLoadsEncoder subscriptionsEncoder = body
                .cpuUtilization(metrics.getCpuUtilization())
                .subscriptionLoadsCount(subscriptionLoads.size());

        for (Map.Entry<SubscriptionId, SubscriptionLoad> entry : subscriptionLoads.entrySet()) {
            SubscriptionId subscriptionId = entry.getKey();
            SubscriptionLoad load = entry.getValue();
            ThroughputEncoder throughputEncoder = subscriptionsEncoder.next()
                    .id(subscriptionId.getValue())
                    .loadStatus(mapLoadStatus(load.getLoadStatus()))
                    .throughput();
            throughputEncoder
                    .rateIn(load.getThroughput().getRateIn())
                    .rateOut(load.getThroughput().getRateOut());
        }

        int len = headerEncoder.encodedLength() + body.encodedLength();

        byte[] dst = new byte[len];
        buffer.getBytes(0, dst);
        return dst;
    }

    private Map<SubscriptionId, SubscriptionLoad> mapToSubscriptionIds(WorkloadMetricsSnapshot metrics) {
        Map<SubscriptionId, SubscriptionLoad> subscriptionLoads = new HashMap<>();
        for (Map.Entry<SubscriptionName, SubscriptionLoad> entry : metrics.getLoads().entrySet()) {
            Optional<SubscriptionId> subscriptionId = subscriptionIds.getSubscriptionId(entry.getKey());
            subscriptionId.ifPresent(id -> subscriptionLoads.put(id, entry.getValue()));
        }
        return subscriptionLoads;
    }

    private SubscriptionLoadStatus mapLoadStatus(LoadStatus loadStatus) {
        switch (loadStatus) {
            case OVERLOADED:
                return OVERLOADED;
            case UNDERLOADED:
                return UNDERLOADED;
            case NORMAL:
            default:
                return NORMAL;
        }
    }
}
