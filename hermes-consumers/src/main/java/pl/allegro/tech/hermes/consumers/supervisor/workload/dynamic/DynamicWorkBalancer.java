package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignment;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignmentView;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancer;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancingResult;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkloadConstraints;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class DynamicWorkBalancer implements WorkBalancer {

    private static final Logger logger = LoggerFactory.getLogger(DynamicWorkBalancer.class);

    private final WorkloadMetricsRegistry workloadMetricsRegistry;
    private final int initialConsumersPerSubscriptions;
    private final int maxSubscriptionsPerConsumer;
    private final Clock clock;
    private final Duration balanceDelay;
    private final PartitionCountProvider partitionCountProvider;
    private final ScalingCoefficientsCalculator scalingCoefficientsCalculator;

    private Instant lastRebalance;

    public DynamicWorkBalancer(WorkloadMetricsRegistry workloadMetricsRegistry,
                               int initialConsumersPerSubscriptions,
                               int maxSubscriptionsPerConsumer,
                               Clock clock,
                               Duration balanceDelay,
                               PartitionCountProvider partitionCountProvider) {
        this.workloadMetricsRegistry = workloadMetricsRegistry;
        this.initialConsumersPerSubscriptions = initialConsumersPerSubscriptions;
        this.maxSubscriptionsPerConsumer = maxSubscriptionsPerConsumer;
        this.clock = clock;
        this.balanceDelay = balanceDelay;
        this.partitionCountProvider = partitionCountProvider;
        this.scalingCoefficientsCalculator = new RemainingCapacityScalingCoefficientsCalculator();
    }

    @Override
    public WorkBalancingResult balance(List<SubscriptionName> subscriptions,
                                       List<String> activeConsumerNodes,
                                       SubscriptionAssignmentView currentState,
                                       WorkloadConstraints constraints) {
        if (lastRebalance == null) {
            lastRebalance = clock.instant();
        }

        Set<ConsumerLoad> consumerLoads = new HashSet<>();
        Map<SubscriptionName, LoadStatusVotes> currentVotes = new HashMap<>();
        for (String consumerId : activeConsumerNodes) {
            ConsumerLoad consumerLoad = new ConsumerLoad(consumerId);
            consumerLoads.add(consumerLoad);
            WorkloadMetricsSnapshot workloadMetricsSnapshot = workloadMetricsRegistry.get(consumerId);
            if (workloadMetricsSnapshot.isDefined() && isValid(consumerId)) {
                for (Map.Entry<SubscriptionName, SubscriptionLoad> loadStatus : workloadMetricsSnapshot.getLoads().entrySet()) {
                    if (isValid(loadStatus.getKey())) {
                        LoadStatusVotes loadStatusVotes = currentVotes.computeIfAbsent(loadStatus.getKey(), subscriptionName -> new LoadStatusVotes());
                        loadStatusVotes.increment(loadStatus.getValue().getLoadStatus());
                        consumerLoad.assign(loadStatus.getKey(), loadStatus.getValue().getThroughput());
                    }
                }
            }
        }

        Map<SubscriptionName, Integer> consumerTasks = new HashMap<>();
        for (SubscriptionName subscriptionName : subscriptions) {
            int assignmentsCount = currentState.getAssignmentsCount(subscriptionName);
            Optional<Integer> overwrittenConsumerCount = constraints.getOverwrittenConsumerCount(subscriptionName);
            LoadStatusVotes loadStatusVotes = currentVotes.get(subscriptionName);
            if (overwrittenConsumerCount.isPresent()) {
                consumerTasks.put(subscriptionName, Math.min(overwrittenConsumerCount.get(), activeConsumerNodes.size()));
            } else if (assignmentsCount == 0) {
                consumerTasks.put(subscriptionName, initialConsumersPerSubscriptions);
            } else if (loadStatusVotes == null) {
                consumerTasks.put(subscriptionName, assignmentsCount);
            } else if (loadStatusVotes.atLeastOneOverloaded()) {
                int maxConsumersForTopic = partitionCountProvider.provide(subscriptionName.getTopicName()).orElse(assignmentsCount);
                int maxConsumers = Math.min(activeConsumerNodes.size(), maxConsumersForTopic);
                consumerTasks.put(subscriptionName, Math.min(++assignmentsCount, maxConsumers));
            } else if (loadStatusVotes.allAgreeThatUnderloaded(assignmentsCount)) {
                consumerTasks.put(subscriptionName, Math.max(1, --assignmentsCount));
            } else {
                consumerTasks.put(subscriptionName, assignmentsCount);
            }
        }

        SubscriptionAssignmentView balancedState = currentState.transform((state, transformer) -> {
            findRemovedSubscriptions(currentState, subscriptions).forEach(transformer::removeSubscription);
            findInactiveConsumers(currentState, activeConsumerNodes).forEach(transformer::removeConsumerNode);
            findNewSubscriptions(currentState, subscriptions).forEach(transformer::addSubscription);
            findNewConsumers(currentState, activeConsumerNodes).forEach(transformer::addConsumerNode);
            minimizeWorkload(state, transformer, consumerTasks);
            availableWorkStream(state, consumerTasks)
                    .forEach(transformer::addAssignment);
            if (isValid(consumerLoads, state)) {
                equalizeWorkload(consumerLoads, transformer);
            }
        });

        return new WorkBalancingResult(balancedState, 0);
    }

    private boolean isValid(Set<ConsumerLoad> consumerLoads, SubscriptionAssignmentView state) {
        if (consumerLoads.size() != state.getConsumerNodes().size()) {
            return false;
        }
        for (ConsumerLoad load : consumerLoads) {
            if (!load.getSubscriptions().equals(state.getSubscriptionsForConsumerNode(load.getConsumerId()))) {
                return false;
            }
        }
        return true;
    }

    private void equalizeWorkload(Set<ConsumerLoad> consumerLoads,
                                  SubscriptionAssignmentView.Transformer transformer) {
        Throughput totalCapacity = consumerLoads.stream()
                .map(ConsumerLoad::throughput)
                .reduce(Throughput.ZERO, Throughput::add);
        Throughput capacityPerBin = totalCapacity.divide(consumerLoads.size());

        String infoBeforeBalance = consumerLoads.stream()
                .map(c -> c.getConsumerId() + ": " + c.throughput())
                .collect(Collectors.joining("\n"));
        logger.info("Before balance: \n{}", infoBeforeBalance);

        for (ConsumerLoad consumer : consumerLoads) {
            if (!consumer.throughput().isLessOrEqualTo(capacityPerBin)) {
                Throughput originalLoad = consumer.throughput();

                Map<SubscriptionName, Throughput> tasksToBalance = consumer.tasks();
                consumer.removeAllTasks();

                Throughput capacity = consumerLoads.stream()
                        .map(c -> capacityPerBin.subtract(c.throughput()))
                        .reduce(Throughput.ZERO, Throughput::add);
                Throughput requirement = tasksToBalance.values().stream().reduce(Throughput.ZERO, Throughput::add);

                ScalingCoefficients scalingCoefficients = scalingCoefficientsCalculator.calculate(capacity, requirement);

                List<SubscriptionName> remainingTasks = tasksToBalance.entrySet().stream()
                        .sorted(((Comparator<Map.Entry<SubscriptionName, Throughput>>) (o1, o2) -> {
                            double o1Weight = scalingCoefficients.scale(o1.getValue());
                            double o2Weight = scalingCoefficients.scale(o2.getValue());
                            return Double.compare(o1Weight, o2Weight);
                        }).reversed())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                List<ConsumerLoad> candidates = consumerLoads.stream()
                        .sorted((o1, o2) -> {
                            double o1Weight = scalingCoefficients.scale(o1.throughput());
                            double o2Weight = scalingCoefficients.scale(o2.throughput());
                            return Double.compare(o1Weight, o2Weight);
                        })
                        .collect(Collectors.toList());

                Iterator<SubscriptionName> iterator = remainingTasks.iterator();
                while (iterator.hasNext()) {
                    SubscriptionName task = iterator.next();
                    for (ConsumerLoad candidate : candidates) {
                        if (candidate.isAssigned(task) || candidate.getTaskCount() >= maxSubscriptionsPerConsumer) {
                            continue;
                        }
                        Throughput taskThroughput = tasksToBalance.get(task);
                        Throughput predictedThroughput = candidate.throughput().add(taskThroughput);
                        if (predictedThroughput.isLessOrEqualTo(originalLoad)) {
                            candidate.assign(task, taskThroughput);
                            transformer.transferAssignment(consumer.getConsumerId(), candidate.getConsumerId(), task);
                            iterator.remove();
                            candidates = consumerLoads.stream()
                                    .sorted((o1, o2) -> {
                                        double o1Weight = scalingCoefficients.scale(o1.throughput());
                                        double o2Weight = scalingCoefficients.scale(o2.throughput());
                                        return Double.compare(o1Weight, o2Weight);
                                    })
                                    .collect(Collectors.toList());
                            break;
                        }
                    }
                }
                for (SubscriptionName subscriptionName : remainingTasks) {
                    Throughput taskThroughput = tasksToBalance.get(subscriptionName);
                    consumer.assign(subscriptionName, taskThroughput);
                }
            }
        }

        String infoAfterBalance = consumerLoads.stream()
                .map(c -> c.getConsumerId() + ": " + c.throughput())
                .collect(Collectors.joining("\n"));
        logger.info("After balance: \n{}", infoAfterBalance);

    }

    private void minimizeWorkload(SubscriptionAssignmentView state,
                                  SubscriptionAssignmentView.Transformer transformer,
                                  Map<SubscriptionName, Integer> consumerTasks) {
        state.getSubscriptions()
                .stream()
                .flatMap(subscriptionName -> findRedundantAssignments(state, subscriptionName, consumerTasks))
                .forEach(transformer::removeAssignment);
    }

    private Stream<SubscriptionAssignment> findRedundantAssignments(SubscriptionAssignmentView state,
                                                                    SubscriptionName subscriptionName,
                                                                    Map<SubscriptionName, Integer> consumerTasks) {
        int assignedConsumers = state.getAssignmentsCountForSubscription(subscriptionName);
        int requiredConsumers = consumerTasks.get(subscriptionName);
        int redundantConsumers = assignedConsumers - requiredConsumers;
        if (redundantConsumers > 0) {
            Stream.Builder<SubscriptionAssignment> redundant = Stream.builder();
            Iterator<SubscriptionAssignment> iterator = state.getAssignmentsForSubscription(subscriptionName).iterator();
            while (redundantConsumers > 0 && iterator.hasNext()) {
                SubscriptionAssignment assignment = iterator.next();
                redundant.add(assignment);
                redundantConsumers--;
            }
            return redundant.build();
        }
        return Stream.empty();
    }

    private List<SubscriptionName> findRemovedSubscriptions(SubscriptionAssignmentView state,
                                                            List<SubscriptionName> subscriptions) {
        return state.getSubscriptions().stream().filter(s -> !subscriptions.contains(s)).collect(toList());
    }

    private List<String> findInactiveConsumers(SubscriptionAssignmentView state, List<String> activeConsumers) {
        return state.getConsumerNodes().stream().filter(c -> !activeConsumers.contains(c)).collect(toList());
    }

    private List<SubscriptionName> findNewSubscriptions(SubscriptionAssignmentView state,
                                                        List<SubscriptionName> subscriptions) {
        return subscriptions.stream().filter(s -> !state.getSubscriptions().contains(s)).collect(toList());
    }

    private List<String> findNewConsumers(SubscriptionAssignmentView state, List<String> activeConsumers) {
        return activeConsumers.stream().filter(c -> !state.getConsumerNodes().contains(c)).collect(toList());
    }


    private boolean isValid(String consumerId) {
        return isReady(lastRebalance);
    }

    private boolean isValid(SubscriptionName subscriptionName) {
        return isReady(lastRebalance);
    }

    private boolean isReady(Instant timestamp) {
        if (timestamp == null) {
            return true;
        }
        Instant now = clock.instant();
        return now.isAfter(timestamp.plus(balanceDelay));
    }

    Stream<SubscriptionAssignment> availableWorkStream(SubscriptionAssignmentView state, Map<SubscriptionName, Integer> consumerTasks) {
        AvailableWork work = new AvailableWork(state, consumerTasks);
        return StreamSupport.stream(work, false);
    }

    class AvailableWork extends Spliterators.AbstractSpliterator<SubscriptionAssignment> {

        private final SubscriptionAssignmentView state;
        private final Map<SubscriptionName, Integer> consumerTasks;

        AvailableWork(SubscriptionAssignmentView state, Map<SubscriptionName, Integer> consumerTasks) {
            super(Long.MAX_VALUE, 0);
            this.state = state;
            this.consumerTasks = consumerTasks;
        }

        @Override
        public boolean tryAdvance(Consumer<? super SubscriptionAssignment> action) {
            Set<String> availableConsumers = availableConsumerNodes(state);
            if (!availableConsumers.isEmpty()) {
                Optional<SubscriptionAssignment> subscriptionAssignment = getNextSubscription(state, availableConsumers)
                        .map(subscription -> getNextSubscriptionAssignment(state, availableConsumers, subscription));
                if (subscriptionAssignment.isPresent()) {
                    action.accept(subscriptionAssignment.get());
                    return true;
                }
            }
            return false;
        }

        private Optional<SubscriptionName> getNextSubscription(SubscriptionAssignmentView state, Set<String> availableConsumerNodes) {
            return state.getSubscriptions().stream()
                    .filter(s -> state.getAssignmentsCountForSubscription(s) < consumerTasks.get(s))
                    .filter(s -> !Sets.difference(availableConsumerNodes, state.getConsumerNodesForSubscription(s)).isEmpty())
                    .min(Comparator.comparingInt(state::getAssignmentsCountForSubscription));
        }

        private SubscriptionAssignment getNextSubscriptionAssignment(SubscriptionAssignmentView state,
                                                                     Set<String> availableConsumerNodes,
                                                                     SubscriptionName subscriptionName) {
            return availableConsumerNodes.stream()
                    .filter(s -> !state.getSubscriptionsForConsumerNode(s).contains(subscriptionName))
                    .min(Comparator.comparingInt(state::getAssignmentsCountForConsumerNode))
                    .map(s -> new SubscriptionAssignment(s, subscriptionName))
                    .get();
        }

        private Set<String> availableConsumerNodes(SubscriptionAssignmentView state) {
            return state.getConsumerNodes().stream()
                    .filter(s -> state.getAssignmentsCountForConsumerNode(s) < maxSubscriptionsPerConsumer)
                    .filter(s -> state.getAssignmentsCountForConsumerNode(s) < state.getSubscriptionsCount())
                    .collect(toSet());
        }
    }
}
