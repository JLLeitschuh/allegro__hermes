package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.kafka.KafkaTopics;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaPartitionCountProvider implements PartitionCountProvider {

    private final KafkaNamesMapper kafkaNamesMapper;
    private final TopicRepository topicRepository;
    private final LoadingCache<TopicName, Integer> partitionCountCache;
    private final AdminClient adminClient;

    public KafkaPartitionCountProvider(KafkaNamesMapper kafkaNamesMapper,
                                       TopicRepository topicRepository,
                                       AdminClient adminClient,
                                       Duration partitionsCacheMaxAge) {
        this.kafkaNamesMapper = kafkaNamesMapper;
        this.topicRepository = topicRepository;
        this.adminClient = adminClient;
        this.partitionCountCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(partitionsCacheMaxAge.toMillis(), MILLISECONDS)
                .build(new PartitionCountLoader());
    }

    @Override
    public Optional<Integer> provide(TopicName topicName) {
        try {
            return Optional.of(partitionCountCache.get(topicName));
        } catch (ExecutionException e) {
            return Optional.empty();
        }
    }

    private class PartitionCountLoader extends CacheLoader<TopicName, Integer> {

        @Override
        public Integer load(TopicName topicName) throws Exception {
            Topic topic = topicRepository.getTopicDetails(topicName);
            KafkaTopics kafkaTopics = kafkaNamesMapper.toKafkaTopics(topic);
            List<String> topics = new ArrayList<>();
            topics.add(kafkaTopics.getPrimary().name().asString());
            if (kafkaTopics.getSecondary().isPresent()) {
                topics.add(kafkaTopics.getSecondary().get().name().asString());
            }
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
            Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();
            return topicDescriptions.values().stream().mapToInt(t -> t.partitions().size()).sum();
        }
    }
}
