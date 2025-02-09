package pl.allegro.tech.hermes.consumers.config;

import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.client.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.message.undelivered.UndeliveredMessageLog;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.common.metric.executor.InstrumentedExecutorServiceFactory;
import pl.allegro.tech.hermes.consumers.consumer.ConsumerAuthorizationHandler;
import pl.allegro.tech.hermes.consumers.consumer.ConsumerMessageSenderFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.ByteBufferMessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.converter.AvroToJsonMessageConverter;
import pl.allegro.tech.hermes.consumers.consumer.converter.DefaultMessageConverterResolver;
import pl.allegro.tech.hermes.consumers.consumer.converter.MessageConverterResolver;
import pl.allegro.tech.hermes.consumers.consumer.converter.NoOperationMessageConverter;
import pl.allegro.tech.hermes.consumers.consumer.interpolation.MessageBodyInterpolator;
import pl.allegro.tech.hermes.consumers.consumer.interpolation.UriInterpolator;
import pl.allegro.tech.hermes.consumers.consumer.offset.ConsumerPartitionAssignmentState;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetQueue;
import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimitSupervisor;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculatorFactory;
import pl.allegro.tech.hermes.consumers.consumer.rate.maxrate.MaxRatePathSerializer;
import pl.allegro.tech.hermes.consumers.consumer.rate.maxrate.MaxRateProviderFactory;
import pl.allegro.tech.hermes.consumers.consumer.rate.maxrate.MaxRateRegistry;
import pl.allegro.tech.hermes.consumers.consumer.rate.maxrate.MaxRateSupervisor;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSenderFactory;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;
import pl.allegro.tech.hermes.consumers.consumer.sender.http.HttpClientsFactory;
import pl.allegro.tech.hermes.consumers.consumer.sender.timeout.FutureAsyncTimeout;
import pl.allegro.tech.hermes.consumers.registry.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.subscription.id.SubscriptionIds;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ClusterAssignmentCache;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ConsumerAssignmentCache;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;
import pl.allegro.tech.hermes.tracker.consumers.LogRepository;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import java.time.Clock;
import java.util.List;

@Configuration
public class ConsumerConfiguration {

    @Bean
    public MaxRatePathSerializer maxRatePathSerializer() {
        return new MaxRatePathSerializer();
    }

    @Bean
    public NoOperationMessageConverter noOperationMessageConverter() {
        return new NoOperationMessageConverter();
    }

    @Bean
    public ConsumerPartitionAssignmentState consumerPartitionAssignmentState() {
        return new ConsumerPartitionAssignmentState();
    }

    @Bean
    public MaxRateRegistry maxRateRegistry(ConfigFactory configFactory,
                                           CuratorFramework curator,
                                           ZookeeperPaths zookeeperPaths,
                                           SubscriptionIds subscriptionIds,
                                           ConsumerAssignmentCache assignmentCache,
                                           ClusterAssignmentCache clusterAssignmentCache) {
        return new MaxRateRegistry(
                configFactory,
                clusterAssignmentCache,
                assignmentCache,
                curator,
                zookeeperPaths,
                subscriptionIds
        );
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public MaxRateSupervisor maxRateSupervisor(ConfigFactory configFactory,
                                               ClusterAssignmentCache clusterAssignmentCache,
                                               MaxRateRegistry maxRateRegistry,
                                               ConsumerNodesRegistry consumerNodesRegistry,
                                               SubscriptionsCache subscriptionsCache,
                                               ZookeeperPaths zookeeperPaths,
                                               HermesMetrics metrics,
                                               Clock clock) {
        return new MaxRateSupervisor(
                configFactory,
                clusterAssignmentCache,
                maxRateRegistry,
                consumerNodesRegistry,
                subscriptionsCache,
                zookeeperPaths,
                metrics,
                clock
        );
    }

    @Bean
    public OffsetQueue offsetQueue(HermesMetrics metrics,
                                   ConfigFactory configFactory) {
        return new OffsetQueue(metrics, configFactory);
    }

    @Bean
    public ConsumerRateLimitSupervisor consumerRateLimitSupervisor(ConfigFactory configFactory) {
        return new ConsumerRateLimitSupervisor(configFactory);
    }

    @Bean
    public MaxRateProviderFactory maxRateProviderFactory(ConfigFactory configFactory,
                                                         MaxRateRegistry maxRateRegistry,
                                                         MaxRateSupervisor maxRateSupervisor,
                                                         HermesMetrics metrics) {
        return new MaxRateProviderFactory(configFactory, maxRateRegistry, maxRateSupervisor, metrics);
    }

    @Bean
    public AvroToJsonMessageConverter avroToJsonMessageConverter() {
        return new AvroToJsonMessageConverter();
    }

    @Bean
    public OutputRateCalculatorFactory outputRateCalculatorFactory(ConfigFactory configFactory,
                                                                   MaxRateProviderFactory maxRateProviderFactory) {
        return new OutputRateCalculatorFactory(configFactory, maxRateProviderFactory);
    }

    @Bean
    public MessageBatchFactory messageBatchFactory(HermesMetrics hermesMetrics,
                                                   Clock clock,
                                                   ConfigFactory configFactory) {
        int poolableSize = configFactory.getIntProperty(Configs.CONSUMER_BATCH_POOLABLE_SIZE);
        int maxPoolSize = configFactory.getIntProperty(Configs.CONSUMER_BATCH_MAX_POOL_SIZE);
        return new ByteBufferMessageBatchFactory(poolableSize, maxPoolSize, clock, hermesMetrics);
    }

    @Bean
    public MessageConverterResolver defaultMessageConverterResolver(AvroToJsonMessageConverter avroToJsonMessageConverter,
                                                                    NoOperationMessageConverter noOperationMessageConverter) {
        return new DefaultMessageConverterResolver(avroToJsonMessageConverter, noOperationMessageConverter);
    }

    @Bean(name = "http-1-client")
    public HttpClient http1Client(HttpClientsFactory httpClientsFactory) {
        return httpClientsFactory.createClientForHttp1("jetty-http-client");
    }

    @Bean(name = "oauth-http-client")
    public HttpClient oauthHttpClient(HttpClientsFactory httpClientsFactory) {
        return httpClientsFactory.createClientForHttp1("jetty-http-oauthclient");
    }

    @Bean
    public ConsumerMessageSenderFactory consumerMessageSenderFactory(ConfigFactory configFactory,
                                                                     HermesMetrics hermesMetrics,
                                                                     MessageSenderFactory messageSenderFactory,
                                                                     Trackers trackers,
                                                                     FutureAsyncTimeout<MessageSendingResult> futureAsyncTimeout,
                                                                     UndeliveredMessageLog undeliveredMessageLog, Clock clock,
                                                                     InstrumentedExecutorServiceFactory instrumentedExecutorServiceFactory,
                                                                     ConsumerAuthorizationHandler consumerAuthorizationHandler) {
        return new ConsumerMessageSenderFactory(
                configFactory,
                hermesMetrics,
                messageSenderFactory,
                trackers,
                futureAsyncTimeout,
                undeliveredMessageLog,
                clock,
                instrumentedExecutorServiceFactory,
                consumerAuthorizationHandler
        );
    }

    @Bean
    public UriInterpolator messageBodyInterpolator() {
        return new MessageBodyInterpolator();
    }

    @Bean(destroyMethod = "close")
    public Trackers trackers(List<LogRepository> repositories) {
        return new Trackers(repositories);
    }
}
