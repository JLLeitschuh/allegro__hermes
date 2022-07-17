package pl.allegro.tech.hermes.consumers.consumer.rate;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculator;

public class BatchConsumerRateLimiter implements ConsumerRateLimiter {

    @Override
    public void initialize() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void acquire() {
    }

    @Override
    public void acquireFiltered() {
    }

    @Override
    public void adjustConsumerRate() {
    }

    @Override
    public void updateSubscription(Subscription newSubscription) {
    }

    @Override
    public void registerSuccessfulSending() {
    }

    @Override
    public void registerFailedSending() {
    }

    @Override
    public OutputRateCalculator.Mode getRateMode() {
        return OutputRateCalculator.Mode.NORMAL;
    }
}
