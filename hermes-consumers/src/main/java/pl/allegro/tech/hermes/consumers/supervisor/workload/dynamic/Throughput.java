package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import java.util.Objects;

class Throughput {

    static Throughput UNDEFINED = new Throughput(-1d, -1d);
    static Throughput ZERO = new Throughput(0, 0);

    private final double rateIn;
    private final double rateOut;

    Throughput(double rateIn, double rateOut) {
        this.rateIn = rateIn;
        this.rateOut = rateOut;
    }

    double getRateIn() {
        return rateIn;
    }

    double getRateOut() {
        return rateOut;
    }

    boolean isUndefined() {
        return rateIn < 0 && rateOut < 0;
    }

    Throughput add(Throughput addend) {
        return new Throughput(rateIn + addend.rateIn, rateOut + addend.rateOut);
    }

    Throughput subtract(Throughput subtrahend) {
        return new Throughput(rateIn - subtrahend.rateIn, rateOut - subtrahend.rateOut);
    }

    Throughput divide(double divisor) {
        return new Throughput(rateIn / divisor, rateOut / divisor);
    }

    boolean isLessOrEqualTo(Throughput capacityPerBin) {
        return rateIn <= capacityPerBin.rateIn && rateOut <= capacityPerBin.rateOut;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Throughput that = (Throughput) o;
        return Double.compare(that.rateIn, rateIn) == 0
                && Double.compare(that.rateOut, rateOut) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rateIn, rateOut);
    }

    @Override
    public String toString() {
        return "(in=" + rateIn + ", out=" + rateOut + ")";
    }
}
