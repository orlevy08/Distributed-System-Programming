package bgu.dsp.sarcasm.common;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
public class Review {

    private String id;
    private String link;
    private String title;
    private String text;
    private Integer rating;
    private String author;
    private String date;

    public Review() {}
}
