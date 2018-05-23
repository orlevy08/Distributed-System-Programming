package bgu.dsp.semanticclassification.models;

import javafx.util.Pair;
import lombok.*;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
@ToString(exclude = {"allUnconfirmed", "numOfPatterns", "numOfCore", "numOfUnconfirmed"})
@Builder
public class Cluster {

    private Set<Pair<String, String>> cluster;
    @Getter private boolean allUnconfirmed;
    @Getter private int numOfPatterns;
    @Getter private int numOfCore;
    @Getter private int numOfUnconfirmed;

    public Set<String> getCore() {
        return this.cluster.stream()
                .filter(pair -> pair.getValue().equals("core"))
                .map(pair -> pair.getKey())
                .collect(Collectors.toSet());
    }

    public Set<String> getUnconfirmed() {
        return this.cluster.stream()
                .filter(pair -> pair.getValue().equals("unconfirmed"))
                .map(pair -> pair.getKey())
                .collect(Collectors.toSet());
    }

    public Set<String> getAllPatterns() {
        return this.cluster.stream()
                .map(pair -> pair.getKey())
                .collect(Collectors.toSet());
    }

    public void merge(Cluster other) {
        Set<String> shared = new HashSet<>(this.getAllPatterns());
        shared.retainAll(other.getAllPatterns());
        Set<Pair<String, String>> newCluster = shared.stream()
                .map(pattern -> new Pair<>(pattern, "core"))
                .collect(Collectors.toSet());
        Set<String> unshared1 = new HashSet<>(this.getAllPatterns());
        unshared1.removeAll(other.getAllPatterns());
        newCluster.addAll(unshared1.stream()
                .map(pattern -> new Pair<>(pattern, "unconfirmed"))
                .collect(Collectors.toSet()));
        Set<String> unshared2 = new HashSet<>(other.getAllPatterns());
        unshared2.removeAll(this.getAllPatterns());
        newCluster.addAll(unshared2.stream()
                .map(pattern -> new Pair<>(pattern, "unconfirmed"))
                .collect(Collectors.toSet()));
        this.cluster = newCluster;
        this.allUnconfirmed = false;
        this.numOfPatterns = newCluster.size();
        this.numOfCore = shared.size();
        this.numOfUnconfirmed = unshared1.size() + unshared2.size();
    }
}
