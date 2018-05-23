package bgu.dsp.sarcasm.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Pair<K, V> {

    private K key;
    private V value;

}
