package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

class RemainingCapacityScalingCoefficientsCalculator implements ScalingCoefficientsCalculator {

    @Override
    public ScalingCoefficients calculate(Throughput remainingCapacity, Throughput requirement) {
        return throughput -> {
            double in = 1 / remainingCapacity.getRateIn();
            double out = 1 / remainingCapacity.getRateOut();
            return throughput.getRateIn() * in + throughput.getRateOut() * out;
        };
    }
}
