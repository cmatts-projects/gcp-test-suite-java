package co.cmatts.gcp.datastore.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Siblings {
    private List<Person> fullSiblings;
    private List<Person> stepByFather;
    private List<Person> stepByMother;
    private List<Person> parents;

    public Siblings(Person person, Set<Person> allSiblings, List<Person> parents) {
        fullSiblings = extractFullSiblings(person, allSiblings);
        stepByFather = extractStepSiblingsByFather(person, allSiblings);
        stepByMother = extractStepSiblingsByMother(person, allSiblings);
        this.parents = parents;
    }

    private List<Person> extractStepSiblingsByMother(Person person, Set<Person> allSiblings) {
        return allSiblings.stream()
                .filter(s -> !Objects.equals(s.getFatherId(), person.getFatherId()) &&
                        Objects.equals(s.getMotherId(), person.getMotherId()))
                .sorted((a, b) -> sortByParentYear(a, b, a.getFatherId(), b.getFatherId()))
                .collect(toList());
    }

    private List<Person> extractStepSiblingsByFather(Person person, Set<Person> allSiblings) {
        return allSiblings.stream()
                .filter(s -> Objects.equals(s.getFatherId(), person.getFatherId()) &&
                        !Objects.equals(s.getMotherId(), person.getMotherId()))
                .sorted((a, b) -> sortByParentYear(a, b, a.getMotherId(), b.getMotherId()))
                .collect(toList());
    }

    private List<Person> extractFullSiblings(Person person, Set<Person> allSiblings) {
        return allSiblings.stream()
                .filter(s -> Objects.equals(s.getFatherId(), person.getFatherId()) &&
                        Objects.equals(s.getMotherId(), person.getMotherId()))
                .sorted(this::comparePeople)
                .collect(toList());
    }

    private int comparePeople(Person a, Person b) {
        return a.getYearOfBirth() > b.getYearOfBirth() ||
                (Objects.equals(a.getYearOfBirth(), b.getYearOfBirth()) && StringUtils.compare(a.getId(), b.getId()) > 0) ? 1 : -1;
    }

    private int sortByParentYear(Person a, Person b, String aParentId, String bParentId) {
        return
                (Objects.equals(aParentId, bParentId) &&
                        (b.getYearOfBirth() == null ||
                                (a.getYearOfBirth() != null && (a.getYearOfBirth() > b.getYearOfBirth() ||
                                        (Objects.equals(a.getYearOfBirth(), b.getYearOfBirth()) && StringUtils.compare(a.getId(), b.getId()) > 0)))))
                        ||
                        (!Objects.equals(aParentId, bParentId) &&
                                ((aParentId != null && bParentId == null) || (aParentId != null && StringUtils.compare(aParentId, bParentId) > 0)))
                        ? 1 : -1;
    }
}
