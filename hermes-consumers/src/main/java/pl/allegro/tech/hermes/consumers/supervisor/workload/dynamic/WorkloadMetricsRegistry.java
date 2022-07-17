package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

public interface WorkloadMetricsRegistry {

    void update(WorkloadMetricsSnapshot metrics) throws Exception;

    WorkloadMetricsSnapshot get(String consumerId);
}
