package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionId;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.MessageHeaderDecoder;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.SubscriptionLoadStatus;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.ThroughputDecoder;
import pl.allegro.tech.hermes.consumers.supervisor.workload.sbe.stubs.WorkloadMetricsDecoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.slf4j.LoggerFactory.getLogger;
import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.NORMAL;
import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.OVERLOADED;
import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.UNDERLOADED;

class ConsumerWorkloadMetricsDecoder {

    private static final Logger logger = getLogger(ConsumerWorkloadMetricsDecoder.class);

    private final SubscriptionIds subscriptionIds;

    ConsumerWorkloadMetricsDecoder(SubscriptionIds subscriptionIds) {
        this.subscriptionIds = subscriptionIds;
    }

    WorkloadMetricsSnapshot decode(byte[] bytes) {
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        WorkloadMetricsDecoder body = new WorkloadMetricsDecoder();

        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        header.wrap(buffer, 0);

        if (header.schemaId() != WorkloadMetricsDecoder.SCHEMA_ID || header.templateId() != WorkloadMetricsDecoder.TEMPLATE_ID) {
            logger.warn("Unable to decode workload metrics, schema or template id mismatch. " +
                            "Required by decoder: [schema id={}, template id={}], " +
                            "encoded in payload: [schema id={}, template id={}]",
                    WorkloadMetricsDecoder.SCHEMA_ID, WorkloadMetricsDecoder.TEMPLATE_ID,
                    header.schemaId(), header.templateId());
            return WorkloadMetricsSnapshot.UNDEFINED;
        }

        body.wrap(buffer, header.encodedLength(), header.blockLength(), header.version());

        double cpuUtilization = body.cpuUtilization();

        Map<SubscriptionName, SubscriptionLoad> subscriptionLoads = new HashMap<>();
        for (WorkloadMetricsDecoder.SubscriptionLoadsDecoder subscriptionDecoder : body.subscriptionLoads()) {
            long id = subscriptionDecoder.id();
            Optional<SubscriptionId> subscriptionId = subscriptionIds.getSubscriptionId(id);
            if (subscriptionId.isPresent()) {
                LoadStatus loadStatus = mapLoadStatus(subscriptionDecoder.loadStatus());
                Throughput throughput = mapThroughput(subscriptionDecoder.throughput());
                SubscriptionLoad load = new SubscriptionLoad(loadStatus, throughput);
                subscriptionLoads.put(subscriptionId.get().getSubscriptionName(), load);
            }
        }
        return new WorkloadMetricsSnapshot(cpuUtilization, subscriptionLoads);
    }

    private LoadStatus mapLoadStatus(SubscriptionLoadStatus loadStatus) {
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

    private Throughput mapThroughput(ThroughputDecoder throughputDecoder) {
        return new Throughput(throughputDecoder.rateIn(), throughputDecoder.rateOut());
    }
}
