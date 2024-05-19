package co.cmatts.gcp.firestore.model;

import lombok.*;

@Data
@Builder
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
public class Person implements FirestoreMappedBean {
    private Integer id;
    private String name;
    private Integer yearOfBirth;
    private Integer yearOfDeath;
    private Integer fatherId;
    private Integer motherId;
    private Long version;

    @Override
    public String tableName() {
        return "people";
    }
}
