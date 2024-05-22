package co.cmatts.gcp.datastore.model;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@EqualsAndHashCode(of = "id")
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class Person {
    @Id
    private String id;
    private String name;
    private Integer yearOfBirth;
    private Integer yearOfDeath;
    @Index private String fatherId;
    @Index private String motherId;
    private List<Fact> facts = new ArrayList<>();
}
