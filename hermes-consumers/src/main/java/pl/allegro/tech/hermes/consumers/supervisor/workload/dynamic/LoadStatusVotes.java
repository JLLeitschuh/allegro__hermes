package pl.allegro.tech.hermes.consumers.supervisor.workload.dynamic;

import pl.allegro.tech.hermes.consumers.consumer.load.LoadStatus;

import java.util.HashMap;
import java.util.Map;

class LoadStatusVotes {
    private final Map<LoadStatus, Integer> votes = new HashMap<>();

    void increment(LoadStatus loadStatus) {
        votes.compute(loadStatus, (k, v) -> (v == null) ? 1 : ++v);
    }

    boolean atLeastOneOverloaded() {
        return votes.containsKey(LoadStatus.OVERLOADED);
    }

    boolean allAgreeThatUnderloaded(int assignmentsCount) {
        return !atLeastOneOverloaded() && votes.getOrDefault(LoadStatus.UNDERLOADED, 0) == assignmentsCount;
    }
}
