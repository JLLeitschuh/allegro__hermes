package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

interface ScalingCoefficients {

    double scale(Throughput throughput);
}
