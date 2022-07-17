package pl.allegro.tech.hermes.consumers.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.allegro.tech.hermes.common.admin.zookeeper.ZookeeperAdminCache;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.kafka.offset.SubscriptionOffsetChangeIndicator;
import pl.allegro.tech.hermes.common.message.wrapper.CompositeMessageContentWrapper;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.ConsumerAuthorizationHandler;
import pl.allegro.tech.hermes.consumers.consumer.ConsumerMessageSenderFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.converter.MessageConverterResolver;
import pl.allegro.tech.hermes.consumers.consumer.load.SubscriptionLoadReporter;
import pl.allegro.tech.hermes.consumers.consumer.offset.ConsumerPartitionAssignmentState;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetQueue;
import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimitSupervisor;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculatorFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.ReceiverFactory;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSenderFactory;
import pl.allegro.tech.hermes.consumers.health.ConsumerMonitor;
import pl.allegro.tech.hermes.consumers.message.undelivered.UndeliveredMessageLogPersister;
import pl.allegro.tech.hermes.consumers.registry.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumerFactory;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersExecutorService;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.consumers.supervisor.NonblockingConsumersSupervisor;
import pl.allegro.tech.hermes.consumers.supervisor.monitor.ConsumersRuntimeMonitor;
import pl.allegro.tech.hermes.consumers.supervisor.process.Retransmitter;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ClusterAssignmentCache;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ConsumerAssignmentCache;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ConsumerAssignmentRegistry;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancer;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkloadSupervisor;
import pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic.DynamicWorkBalancer;
import pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic.KafkaPartitionCountProvider;
import pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic.WorkloadMetricsRegistry;
import pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic.WorkloadMetricsReporter;
import pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic.ZookeeperWorkloadMetricsRegistry;
import pl.allegro.tech.hermes.domain.notifications.InternalNotificationsBus;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraintsRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import java.time.Clock;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.slf4j.LoggerFactory.getLogger;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_ASSIGNMENT_PROCESSING_THREAD_POOL_SIZE;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_ADMIN_REQUEST_TIMEOUT_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_ENABLED;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_MECHANISM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PROTOCOL;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_USERNAME;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_BROKER_LIST;

@Configuration
public class SupervisorConfiguration {
    private static final Logger logger = getLogger(SupervisorConfiguration.class);

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public WorkloadSupervisor workloadSupervisor(InternalNotificationsBus notificationsBus,
                                                 ConsumerNodesRegistry consumerNodesRegistry,
                                                 ConsumerAssignmentRegistry assignmentRegistry,
                                                 ConsumerAssignmentCache consumerAssignmentCache,
                                                 ClusterAssignmentCache clusterAssignmentCache,
                                                 SubscriptionsCache subscriptionsCache,
                                                 ConsumersSupervisor supervisor,
                                                 ZookeeperAdminCache adminCache,
                                                 HermesMetrics metrics,
                                                 ConfigFactory configs,
                                                 WorkloadConstraintsRepository workloadConstraintsRepository,
                                                 WorkBalancer workBalancer) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("AssignmentExecutor-%d")
                .setUncaughtExceptionHandler((t, e) -> logger.error("AssignmentExecutor failed {}", t.getName(), e)).build();
        ExecutorService assignmentExecutor = newFixedThreadPool(configs.getIntProperty(CONSUMER_WORKLOAD_ASSIGNMENT_PROCESSING_THREAD_POOL_SIZE), threadFactory);
        return new WorkloadSupervisor(
                supervisor,
                notificationsBus,
                subscriptionsCache,
                consumerAssignmentCache,
                assignmentRegistry,
                clusterAssignmentCache,
                consumerNodesRegistry,
                adminCache,
                assignmentExecutor,
                configs,
                metrics,
                workloadConstraintsRepository,
                workBalancer
        );
    }

    @Bean
    public WorkBalancer workBalancer(WorkloadMetricsRegistry workloadMetricsRegistry,
                                     Clock clock,
                                     ConfigFactory configFactory,
                                     KafkaNamesMapper kafkaNamesMapper,
                                     TopicRepository topicRepository) {
//        return new FixedWorkBalancer(
//                configFactory.getIntProperty(CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION),
//                configFactory.getIntProperty(CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER)
//        );
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, configFactory.getStringProperty(KAFKA_BROKER_LIST));
        props.put(SECURITY_PROTOCOL_CONFIG, DEFAULT_SECURITY_PROTOCOL);
        props.put(REQUEST_TIMEOUT_MS_CONFIG, configFactory.getIntProperty(KAFKA_ADMIN_REQUEST_TIMEOUT_MS));
        if (configFactory.getBooleanProperty(KAFKA_AUTHORIZATION_ENABLED)) {
            props.put(SASL_MECHANISM, configFactory.getStringProperty(KAFKA_AUTHORIZATION_MECHANISM));
            props.put(SECURITY_PROTOCOL_CONFIG, configFactory.getStringProperty(KAFKA_AUTHORIZATION_PROTOCOL));
            props.put(SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "username=\"" + configFactory.getStringProperty(KAFKA_AUTHORIZATION_USERNAME) + "\"\n"
                            + "password=\"" + configFactory.getStringProperty(KAFKA_AUTHORIZATION_PASSWORD) + "\";"
            );
        }
        AdminClient adminClient = AdminClient.create(props);
        return new DynamicWorkBalancer(
                workloadMetricsRegistry,
                configFactory.getIntProperty(CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION),
                configFactory.getIntProperty(CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER),
                clock,
                Duration.ofMinutes(5),
                new KafkaPartitionCountProvider(kafkaNamesMapper, topicRepository, adminClient, Duration.ofMinutes(60))
        );
    }

    @Bean
    public Retransmitter retransmitter(SubscriptionOffsetChangeIndicator subscriptionOffsetChangeIndicator,
                                       ConfigFactory configs) {
        return new Retransmitter(subscriptionOffsetChangeIndicator, configs);
    }

    @Bean
    public ConsumerFactory consumerFactory(ReceiverFactory messageReceiverFactory,
                                           HermesMetrics hermesMetrics,
                                           ConfigFactory configFactory,
                                           ConsumerRateLimitSupervisor consumerRateLimitSupervisor,
                                           OutputRateCalculatorFactory outputRateCalculatorFactory,
                                           Trackers trackers,
                                           OffsetQueue offsetQueue,
                                           ConsumerMessageSenderFactory consumerMessageSenderFactory,
                                           TopicRepository topicRepository,
                                           MessageConverterResolver messageConverterResolver,
                                           MessageBatchFactory byteBufferMessageBatchFactory,
                                           CompositeMessageContentWrapper compositeMessageContentWrapper,
                                           MessageBatchSenderFactory batchSenderFactory,
                                           ConsumerAuthorizationHandler consumerAuthorizationHandler,
                                           Clock clock,
                                           SubscriptionLoadReporter subscriptionLoadReporter) {
        return new ConsumerFactory(
                messageReceiverFactory,
                hermesMetrics,
                configFactory,
                consumerRateLimitSupervisor,
                outputRateCalculatorFactory,
                trackers,
                offsetQueue,
                consumerMessageSenderFactory,
                topicRepository,
                messageConverterResolver,
                byteBufferMessageBatchFactory,
                compositeMessageContentWrapper,
                batchSenderFactory,
                consumerAuthorizationHandler,
                clock,
                subscriptionLoadReporter
        );
    }

    @Bean
    public ConsumersExecutorService consumersExecutorService(ConfigFactory configFactory,
                                                             HermesMetrics hermesMetrics) {
        return new ConsumersExecutorService(configFactory, hermesMetrics);
    }

    @Bean
    public ConsumersSupervisor nonblockingConsumersSupervisor(ConfigFactory configFactory,
                                                              ConsumersExecutorService executor,
                                                              ConsumerFactory consumerFactory,
                                                              OffsetQueue offsetQueue,
                                                              ConsumerPartitionAssignmentState consumerPartitionAssignmentState,
                                                              Retransmitter retransmitter,
                                                              UndeliveredMessageLogPersister undeliveredMessageLogPersister,
                                                              SubscriptionRepository subscriptionRepository,
                                                              HermesMetrics metrics,
                                                              ConsumerMonitor monitor,
                                                              Clock clock) {
        return new NonblockingConsumersSupervisor(configFactory, executor, consumerFactory, offsetQueue,
                consumerPartitionAssignmentState, retransmitter, undeliveredMessageLogPersister,
                subscriptionRepository, metrics, monitor, clock);
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public ConsumersRuntimeMonitor consumersRuntimeMonitor(ConsumersSupervisor consumerSupervisor,
                                                           WorkloadSupervisor workloadSupervisor,
                                                           HermesMetrics hermesMetrics,
                                                           SubscriptionsCache subscriptionsCache,
                                                           ConfigFactory configFactory) {
        return new ConsumersRuntimeMonitor(
                consumerSupervisor,
                workloadSupervisor,
                hermesMetrics,
                subscriptionsCache,
                configFactory
        );
    }

    @Bean
    public ConsumerAssignmentRegistry consumerAssignmentRegistry(CuratorFramework curator,
                                                                 ConfigFactory configFactory,
                                                                 ZookeeperPaths zookeeperPaths,
                                                                 SubscriptionIds subscriptionIds) {
        return new ConsumerAssignmentRegistry(curator, configFactory, zookeeperPaths, subscriptionIds);
    }

    @Bean
    public ClusterAssignmentCache clusterAssignmentCache(CuratorFramework curator,
                                                         ConfigFactory configFactory,
                                                         ZookeeperPaths zookeeperPaths,
                                                         SubscriptionIds subscriptionIds,
                                                         ConsumerNodesRegistry consumerNodesRegistry) {
        String clusterName = configFactory.getStringProperty(Configs.KAFKA_CLUSTER_NAME);
        return new ClusterAssignmentCache(curator, clusterName, zookeeperPaths, subscriptionIds, consumerNodesRegistry);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public ConsumerAssignmentCache consumerAssignmentCache(CuratorFramework curator,
                                                           ConfigFactory configFactory,
                                                           ZookeeperPaths zookeeperPaths,
                                                           SubscriptionIds subscriptionIds) {
        String consumerId = configFactory.getStringProperty(Configs.CONSUMER_WORKLOAD_NODE_ID);
        String clusterName = configFactory.getStringProperty(Configs.KAFKA_CLUSTER_NAME);
        return new ConsumerAssignmentCache(curator, consumerId, clusterName, zookeeperPaths, subscriptionIds);
    }

    @Bean
    public WorkloadMetricsRegistry workloadMetricsRegistry(CuratorFramework curator,
                                                           SubscriptionIds subscriptionIds,
                                                           ZookeeperPaths zookeeperPaths,
                                                           ConfigFactory configFactory) {
        String consumerId = configFactory.getStringProperty(Configs.CONSUMER_WORKLOAD_NODE_ID);
        String clusterName = configFactory.getStringProperty(Configs.KAFKA_CLUSTER_NAME);
        return new ZookeeperWorkloadMetricsRegistry(curator, subscriptionIds, zookeeperPaths, consumerId, clusterName, 100_000);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public WorkloadMetricsReporter workloadMetricsReporter(ConsumerAssignmentCache consumerAssignmentCache,
                                                           WorkloadMetricsRegistry workloadMetricsRegistry,
                                                           Clock clock) {
        return new WorkloadMetricsReporter(Duration.ofMinutes(1), Duration.ofMinutes(10), clock, consumerAssignmentCache, workloadMetricsRegistry);
    }
}
