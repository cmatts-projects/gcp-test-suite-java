package co.cmatts.gcp.firestore.model;

import lombok.*;

@Data
@Builder
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
public class Fact implements FirestoreMappedBean {
    private String id;
    private String personId;
    private Integer year;
    private String image;
    private String source;
    private String description;
    private Long version;

    @Override
    public String tableName() {
        return "facts";
    }
}
