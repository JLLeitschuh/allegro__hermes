package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;
import pl.allegro.tech.hermes.consumers.consumer.load.SubscriptionLoadReporter;
import pl.allegro.tech.hermes.consumers.supervisor.workload.ConsumerAssignmentCache;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus.NORMAL;

public class WorkloadMetricsReporter implements SubscriptionLoadReporter {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadMetricsReporter.class);

    private final OperatingSystemMXBean platformMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    private final RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    private final Duration interval;
    private final Duration loadStatusUpdateDelay;
    private final Clock clock;
    private final ConsumerAssignmentCache consumerAssignmentCache;
    private final WorkloadMetricsRegistry workloadMetricsRegistry;
    private final ScheduledExecutorService executor;

    private final Map<SubscriptionName, OngoingLoadStatus> ongoingLoadStatuses = new ConcurrentHashMap<>();
    private final Map<SubscriptionName, SubscriptionMessagesCounter> subscriptionMessageCounters = new ConcurrentHashMap<>();
    private volatile long lastProcessCpuTime = 0;
    private volatile long lastReset = 0;

    public WorkloadMetricsReporter(Duration interval,
                                   Duration loadStatusUpdateDelay,
                                   Clock clock,
                                   ConsumerAssignmentCache consumerAssignmentCache,
                                   WorkloadMetricsRegistry workloadMetricsRegistry) {
        this.interval = interval;
        this.loadStatusUpdateDelay = loadStatusUpdateDelay;
        this.clock = clock;
        this.consumerAssignmentCache = consumerAssignmentCache;
        this.workloadMetricsRegistry = workloadMetricsRegistry;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("workload-metrics-reporter-%d").build()
        );
    }

    public void start() {
        executor.scheduleWithFixedDelay(this::report, 0, interval.getSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdown();
    }

    private void report() {
        try {
            long uptime = runtimeMxBean.getUptime();
            long elapsedMillis = uptime - lastReset;
            lastReset = uptime;

            WorkloadMetricsSnapshot workloadMetricsSnapshot = new WorkloadMetricsSnapshot(
                    calculateCpuUtilization(elapsedMillis),
                    calculateLoads(elapsedMillis)
            );
            workloadMetricsRegistry.update(workloadMetricsSnapshot);

        } catch (Exception e) {
            logger.error("Error while reporting workload metrics", e);
        }
    }

    private double calculateCpuUtilization(long elapsedMillis) {
        long elapsedNanoseconds = TimeUnit.MILLISECONDS.toNanos(elapsedMillis);
        long processCpuTime = platformMXBean.getProcessCpuTime();
        double cpuUtilization = (processCpuTime - lastProcessCpuTime) / (double) elapsedNanoseconds;
        lastProcessCpuTime = processCpuTime;
        return cpuUtilization;
    }

    private Map<SubscriptionName, SubscriptionLoad> calculateLoads(long elapsedMillis) {
        Set<SubscriptionName> assignedSubscriptions = consumerAssignmentCache.getConsumerSubscriptions();
        subscriptionMessageCounters.entrySet().removeIf(e -> !assignedSubscriptions.contains(e.getKey()));
        ongoingLoadStatuses.entrySet().removeIf(e -> !assignedSubscriptions.contains(e.getKey()));
        Map<SubscriptionName, SubscriptionLoad> loads = new HashMap<>();
        for (SubscriptionName subscriptionName : assignedSubscriptions) {
            SubscriptionLoad load = new SubscriptionLoad(
                    calculateLoadStatus(subscriptionName),
                    calculatePartitionThroughput(subscriptionName, elapsedMillis)
            );
            loads.put(subscriptionName, load);
        }
        return loads;
    }

    private LoadStatus calculateLoadStatus(SubscriptionName subscriptionName) {
        OngoingLoadStatus ongoingLoadStatus = ongoingLoadStatuses.get(subscriptionName);
        if (ongoingLoadStatus != null && ongoingLoadStatus.isReady()) {
            return ongoingLoadStatus.loadStatus;
        }
        return NORMAL;
    }

    private Throughput calculatePartitionThroughput(SubscriptionName subscriptionName, long elapsedMillis) {
        SubscriptionMessagesCounter subscriptionMessagesCounter = subscriptionMessageCounters.get(subscriptionName);
        if (subscriptionMessagesCounter != null) {
            return subscriptionMessagesCounter.calculateThroughput(elapsedMillis);
        }
        return Throughput.UNDEFINED;
    }

    @Override
    public void recordStatus(SubscriptionName subscriptionName, LoadStatus loadStatus) {
        OngoingLoadStatus ongoingLoadStatus = ongoingLoadStatuses.get(subscriptionName);
        if (ongoingLoadStatus == null || ongoingLoadStatus.loadStatus != loadStatus) {
            ongoingLoadStatuses.put(subscriptionName, new OngoingLoadStatus(loadStatus, clock.instant()));
        }
    }

    @Override
    public void recordMessagesOut(SubscriptionName subscriptionName, int count) {
        getMessageCounter(subscriptionName).recordOut(count);
    }

    @Override
    public void recordMessagesIn(SubscriptionName subscriptionName, int count) {
        getMessageCounter(subscriptionName).recordIn(count);
    }

    private SubscriptionMessagesCounter getMessageCounter(SubscriptionName subscriptionName) {
        return subscriptionMessageCounters.computeIfAbsent(subscriptionName, ignore -> new SubscriptionMessagesCounter());
    }

    private static class SubscriptionMessagesCounter {
        private final LongAdder messagesIn = new LongAdder();
        private final LongAdder messagesOut = new LongAdder();

        void recordIn(int count) {
            messagesIn.add(count);
        }

        void recordOut(int count) {
            messagesOut.add(count);
        }

        Throughput calculateThroughput(long elapsedMillis) {
            long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedMillis);
            long period = Math.max(elapsedSeconds, 1);
            return new Throughput(
                    (double) messagesIn.sumThenReset() / period,
                    (double) messagesOut.sumThenReset() / period
            );
        }
    }

    private class OngoingLoadStatus {
        private final LoadStatus loadStatus;
        private final Instant timestamp;

        private OngoingLoadStatus(LoadStatus loadStatus, Instant timestamp) {
            this.loadStatus = loadStatus;
            this.timestamp = timestamp;
        }

        public boolean isReady() {
            Instant now = clock.instant();
            return now.isAfter(timestamp.plus(loadStatusUpdateDelay));
        }
    }
}
