package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

public class ZookeeperWorkloadMetricsRegistry implements WorkloadMetricsRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWorkloadMetricsRegistry.class);

    private final CuratorFramework curator;
    private final ZookeeperPaths zookeeperPaths;
    private final String basePath;
    private final String currentConsumerPath;
    private final ConsumerWorkloadMetricsEncoder encoder;
    private final ConsumerWorkloadMetricsDecoder decoder;

    public ZookeeperWorkloadMetricsRegistry(CuratorFramework curator,
                                            SubscriptionIds subscriptionIds,
                                            ZookeeperPaths zookeeperPaths,
                                            String currentConsumerId,
                                            String clusterName,
                                            int sbeEncoderBufferSize) {
        this.curator = curator;
        this.zookeeperPaths = zookeeperPaths;
        this.basePath = zookeeperPaths.join(zookeeperPaths.basePath(), "consumers-workload-experiment", clusterName, "metrics");
        this.currentConsumerPath = zookeeperPaths.join(basePath, currentConsumerId);
        this.encoder = new ConsumerWorkloadMetricsEncoder(subscriptionIds, sbeEncoderBufferSize);
        this.decoder = new ConsumerWorkloadMetricsDecoder(subscriptionIds);
    }

    @Override
    public void update(WorkloadMetricsSnapshot metrics) throws Exception {
        byte[] encoded = encoder.encode(metrics);
        try {
            curator.setData().forPath(currentConsumerPath, encoded);
        } catch (KeeperException.NoNodeException e) {
            try {
                curator.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(currentConsumerPath, encoded);
            } catch (KeeperException.NodeExistsException ex) {
                // ignore
            }
        }
    }

    @Override
    public WorkloadMetricsSnapshot get(String consumerId) {
        String consumerWorkloadMetricsPath = zookeeperPaths.join(basePath, consumerId);
        try {
            if (curator.checkExists().forPath(consumerWorkloadMetricsPath) != null) {
                byte[] bytes = curator.getData().forPath(consumerWorkloadMetricsPath);
                return decoder.decode(bytes);
            }
        } catch (Exception e) {
            logger.warn("Could not read node data on path " + consumerWorkloadMetricsPath, e);
        }
        return WorkloadMetricsSnapshot.UNDEFINED;
    }
}
