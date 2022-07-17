package pl.allegro.tech.hermes.consumers.consumer.rate;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculator;

public interface ConsumerRateLimiter {

    void initialize();

    void shutdown();

    void acquire();

    void acquireFiltered();

    void adjustConsumerRate();

    void updateSubscription(Subscription newSubscription);

    void registerSuccessfulSending();

    void registerFailedSending();

    OutputRateCalculator.Mode getRateMode();
}
