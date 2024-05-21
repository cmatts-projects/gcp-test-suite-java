package co.cmatts.gcp.firestore;

import co.cmatts.gcp.firestore.model.Fact;
import co.cmatts.gcp.firestore.model.Person;
import co.cmatts.gcp.firestore.model.Siblings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import java.util.List;
import java.util.Optional;

import static co.cmatts.gcp.firestore.FirestoreTestDataFactory.*;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
class FirestoreRepositoryTest {

    @SystemStub
    private static SystemProperties systemProperties;

    @Container
    private static final FirestoreEmulatorContainer localFirestore = new FirestoreEmulatorContainer("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    private static FirestoreRepository repo;

    @BeforeAll
    static void beforeAll() throws Exception {
        systemProperties
                .set("local.firestore.url", localFirestore.getEmulatorEndpoint())
                .set("local.project", "test-project");

        repo = new FirestoreRepository();
        repo.load(peopleDataList(), factDataList());
    }

    @Test
    void shouldFindPerson() throws Exception {
        Optional<Person> result = repo.findPerson("1");
        assertThat(result.isPresent()).isTrue();

        Person p = result.get();
        assertThat(p.getId()).isEqualTo("1");
        assertThat(p.getYearOfBirth()).isEqualTo(1900);
        assertThat(p.getYearOfDeath()).isEqualTo(1990);
        assertThat(p.getFatherId()).isEqualTo("16");
        assertThat(p.getMotherId()).isEqualTo("17");
        assertThat(p.toString())
                .isEqualTo(person(1).toString());
    }

    @Test
	void shouldNotFindPerson() throws Exception {
        Optional<Person> result = repo.findPerson("99");
        assertThat(result.isPresent()).isFalse();
    }

    @Test
	void shouldFindFactsForPerson() throws Exception {
        List<Fact> facts = repo.findFacts("1");
        assertThat(facts).hasSize(3);
        assertThat(facts).containsExactlyInAnyOrder(fact(1), fact(2), fact(3));
        assertThat(facts.stream().filter(f -> f.getId().equals("2")).findAny().get().toString())
                .isEqualTo(fact(2).toString());
    }

    @Test
	void shouldNotFindFacts() throws Exception {
        List<Fact> facts = repo.findFacts("99");
        assertThat(facts).hasSize(0);
    }

    @Test
	void shouldFindAllSiblingsGroupedByParentsAndInOrderOfYearOfBirth() throws Exception {
        Siblings siblings = repo.findSiblings("1");
        assertThat(siblings).isEqualTo(PERSON_1_SIBLINGS);
    }

    @Test
	void shouldFindSiblingsWithNoMotherAssumingTheSameMother() throws Exception {
        Siblings siblings = repo.findSiblings("3");

        assertThat(siblings).isEqualTo(PERSON_3_SIBLINGS);
    }

    @Test
	void shouldFindSiblingsWithNoFatherAssumingTheSameFather() throws Exception {
        Siblings siblings = repo.findSiblings("8");

        assertThat(siblings).isEqualTo(PERSON_8_SIBLINGS);
    }

    @Test
	void shouldFindAllPeopleSorted() throws Exception {
        List<Person> people = repo.findPeople();
        assertThat(people).hasSize(peopleCount());
        assertThat(people.get(0).getName()).isEqualTo("First Person");
        assertThat(people.get(1).getName()).isEqualTo("Mr Test");
    }

    @Test
    void shouldUpdateEntities() throws Exception {
        Person person = repo.findPerson("21").get();
        person.setYearOfBirth(1799);
        person.setYearOfDeath(1888);

        Fact fact = repo.findFacts("21").get(0);
        fact.setImage("Updated");
        fact.setDescription("A changed description");

        repo.updateEntities(asList(person, fact));

        Person updatedPerson = repo.findPerson("21").get();
        Fact updatedFact = repo.findFacts("21").get(0);

        assertThat(updatedPerson.toString()).isEqualTo(person.toString());
        assertThat(updatedFact.toString()).isEqualTo(fact.toString());
    }
}
