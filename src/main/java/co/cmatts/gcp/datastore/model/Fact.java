package co.cmatts.gcp.datastore.model;

import lombok.*;

@Data
@Builder
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Fact {
    private Integer year;
    private String image;
    private String source;
    private String description;
}
