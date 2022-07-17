package pl.allegro.tech.hermes.consumers.consumer;

import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;
import pl.allegro.tech.hermes.consumers.consumer.load.SubscriptionLoadReporter;

public class NoOpSubscriptionLoadReporter implements SubscriptionLoadReporter {
    @Override
    public void recordStatus(SubscriptionName subscriptionName, LoadStatus loadStatus) {

    }

    @Override
    public void recordMessagesOut(SubscriptionName subscriptionName, int count) {

    }

    @Override
    public void recordMessagesIn(SubscriptionName subscriptionName, int count) {

    }
}
