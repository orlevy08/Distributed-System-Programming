package bgu.dsp.semanticclassification.mapreduce;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@AllArgsConstructor
@Getter
public class MapredCluster {
    private List<String> core;
    private List<String> unconfirmed;

    public int numOfCore(){
        return core.size();
    }

    public int numOfUnconfirmed(){
        return unconfirmed.size();
    }
}
