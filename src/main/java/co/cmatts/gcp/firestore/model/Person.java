package co.cmatts.gcp.firestore.model;

import lombok.*;

@Data
@Builder
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
public class Person implements FirestoreMappedBean {
    private String id;
    private String name;
    private Integer yearOfBirth;
    private Integer yearOfDeath;
    private String fatherId;
    private String motherId;

    @Override
    public String tableName() {
        return "people";
    }
}
