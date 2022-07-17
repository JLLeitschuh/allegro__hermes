package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

interface ScalingCoefficientsCalculator {

    ScalingCoefficients calculate(Throughput remainingCapacity, Throughput requirement);
}
