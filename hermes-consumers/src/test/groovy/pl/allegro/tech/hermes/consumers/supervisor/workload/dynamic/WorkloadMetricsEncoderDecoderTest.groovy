package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic

import pl.allegro.tech.hermes.api.SubscriptionName
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionId
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds
import pl.allegro.tech.hermes.consumers.supervisor.workload.TestSubscriptionIds
import spock.lang.Specification

import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.NORMAL
import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.OVERLOADED

class WorkloadMetricsEncoderDecoderTest extends Specification {

    def "should encode and decode"() {
        given:
        SubscriptionName sub1 = SubscriptionName.fromString('pl.allegro.tech.consumers$sub1')
        SubscriptionName sub2 = SubscriptionName.fromString('pl.allegro.tech.consumers$sub2')
        SubscriptionIds subscriptionIds = new TestSubscriptionIds([
                SubscriptionId.from(sub1, -1422951212L),
                SubscriptionId.from(sub2, 2L)
        ])
        ConsumerWorkloadMetricsEncoder encoder = new ConsumerWorkloadMetricsEncoder(subscriptionIds, 100)
        ConsumerWorkloadMetricsDecoder decoder = new ConsumerWorkloadMetricsDecoder(subscriptionIds)
        Map<SubscriptionName, SubscriptionLoad> subscriptionLoads = Map.of(
                sub1, new SubscriptionLoad(NORMAL, new Throughput(10, 100)),
                sub2, new SubscriptionLoad(OVERLOADED, new Throughput(200, 200))
        )
        WorkloadMetricsSnapshot workloadMetricsSnapshot = new WorkloadMetricsSnapshot(0.5, subscriptionLoads)

        when:
        byte[] bytes = encoder.encode(workloadMetricsSnapshot)

        then:
        decoder.decode(bytes) == workloadMetricsSnapshot
    }
}
