package pl.allegro.tech.hermes.consumers.consumer.load;

import pl.allegro.tech.hermes.api.SubscriptionName;

public interface SubscriptionLoadReporter {

    void recordStatus(SubscriptionName subscriptionName, LoadStatus loadStatus);

    void recordMessagesOut(SubscriptionName subscriptionName, int count);

    void recordMessagesIn(SubscriptionName subscriptionName, int count);
}
