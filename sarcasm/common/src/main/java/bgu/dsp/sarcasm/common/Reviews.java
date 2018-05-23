package bgu.dsp.sarcasm.common;

import lombok.*;

import java.util.List;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
public class Reviews{

    private String title;
    private List<Review> reviews;

    public Reviews() {}
}
