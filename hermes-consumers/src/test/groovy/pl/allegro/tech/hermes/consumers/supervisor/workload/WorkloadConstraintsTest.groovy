package pl.allegro.tech.hermes.consumers.supervisor.workload

import pl.allegro.tech.hermes.api.SubscriptionName
import pl.allegro.tech.hermes.api.TopicName
import pl.allegro.tech.hermes.api.Constraints
import spock.lang.Specification
import spock.lang.Unroll

import static java.util.Collections.emptyMap

class WorkloadConstraintsTest extends Specification {

    @Unroll
    def "should return constraints of given subscription or empty value if constraints don't exist (#subscriptionName)"() {
        given:
        def workloadConstraints = new WorkloadConstraints(
                [(SubscriptionName.fromString('group.topic$sub1')): new Constraints(3),
                 (SubscriptionName.fromString('group.topic$sub2')): new Constraints(1)],
                emptyMap()
        )

        expect:
        workloadConstraints.getOverwrittenConsumerCount(subscriptionName) == expectedResult

        where:
        subscriptionName                                        | expectedResult
        SubscriptionName.fromString('group.topic$sub1')         | Optional.of(3)
        SubscriptionName.fromString('group.topic$undefined')    | Optional.empty()
    }

    @Unroll
    def "should return subscription constraints or topic constraints if given subscription has no constraints (#subscriptionName)"() {
        def workloadConstraints = new WorkloadConstraints(
                [(SubscriptionName.fromString('group.topic$sub1')): new Constraints(3)],
                [(TopicName.fromQualifiedName('group.topic')): new Constraints(2)]
        )

        expect:
        workloadConstraints.getOverwrittenConsumerCount(subscriptionName) == expectedResult

        where:
        subscriptionName                                | expectedResult
        SubscriptionName.fromString('group.topic$sub1') | Optional.of(3)
        SubscriptionName.fromString('group.topic$sub2') | Optional.of(2)
    }

    @Unroll
    def "should return empty if specified constraints for topic have value less or equal to 0 (#incorrectConsumersNumber)"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.topic$sub')
        def workloadConstraints = new WorkloadConstraints(
                emptyMap(),
                [(TopicName.fromQualifiedName('group.topic')): new Constraints(incorrectConsumersNumber)]
        )

        expect:
        workloadConstraints.getOverwrittenConsumerCount(subscriptionName) == Optional.empty()

        where:
        incorrectConsumersNumber << [0, -1]
    }

    @Unroll
    def "should return empty if specified constraints for subscription have value less or equal to 0 (#incorrectConsumersNumber)"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.incorrect_topic$sub')
        def workloadConstraints = new WorkloadConstraints(
                [(subscriptionName): new Constraints(incorrectConsumersNumber)],
                emptyMap()
        )

        expect:
        workloadConstraints.getOverwrittenConsumerCount(subscriptionName) == Optional.empty()

        where:
        incorrectConsumersNumber << [0, -1]
    }

    @Unroll
    def "should return empty if specified constraints are null"() {
        given:
        def subscriptionName = SubscriptionName.fromString('group.incorrect_topic$sub')
        def workloadConstraints = new WorkloadConstraints(
                constraintsSubscription as Map,
                constraintsTopic as Map
        )

        expect:
        workloadConstraints.getOverwrittenConsumerCount(subscriptionName) == Optional.empty()

        where:
        constraintsSubscription | constraintsTopic
        null                    | emptyMap()
        emptyMap()              | null
    }
}
