package bgu.dsp.semanticclassification.models;

import lombok.Value;

/**
 * Class to hold a tuples of 2 elements
 * @param <T1> The first element class type
 * @param <T2> The second element class type
 */
@Value(staticConstructor = "of")
public class Tuple2<T1,T2> {

    private final T1 t1;
    private final T2 t2;
}
