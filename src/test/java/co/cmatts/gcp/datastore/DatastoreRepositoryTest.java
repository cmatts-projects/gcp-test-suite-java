package co.cmatts.gcp.datastore;

import co.cmatts.gcp.datastore.model.Fact;
import co.cmatts.gcp.datastore.model.Person;
import co.cmatts.gcp.datastore.model.Siblings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.DatastoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import java.util.List;
import java.util.Optional;

import static co.cmatts.gcp.datastore.DatastoreTestDataFactory.*;
import static co.cmatts.gcp.datastore.DatastoreTestDataFactory.peopleCount;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
class DatastoreRepositoryTest   {

    @SystemStub
    private static SystemProperties systemProperties;

    @Container
    private static final DatastoreEmulatorContainer localFirestore = new DatastoreEmulatorContainer("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    private static DatastoreRepository repo;

    @BeforeAll
    static void beforeAll() {
        systemProperties
                .set("local.datastore.url", localFirestore.getEmulatorEndpoint())
                .set("local.project", "test-project");

        repo = new DatastoreRepository();
        repo.save(peopleDataList());
    }

    @Test
    void shouldFindPerson() {
        Optional<Person> result = repo.findPerson("1");
        assertThat(result.isPresent()).isTrue();

        Person p = result.get();
        assertThat(p.getId()).isEqualTo("1");
        assertThat(p.getYearOfBirth()).isEqualTo(1900);
        assertThat(p.getYearOfDeath()).isEqualTo(1990);
        assertThat(p.getFatherId()).isEqualTo("16");
        assertThat(p.getMotherId()).isEqualTo("17");
        assertThat(p.getFacts()).hasSize(3);
        assertThat(p.getFacts()).containsExactlyInAnyOrder(fact(0), fact(1), fact(2));
        assertThat(p.toString()).isEqualTo(person(1).toString());
    }

    @Test
    void shouldNotFindPerson() {
        Optional<Person> result = repo.findPerson("99");
        assertThat(result.isPresent()).isFalse();
    }

    @Test
    void shouldFindAllSiblingsGroupedByParentsAndInOrderOfYearOfBirth() {
        Siblings siblings = repo.findSiblings("1");
        assertThat(siblings).isEqualTo(PERSON_1_SIBLINGS);
    }

    @Test
    void shouldFindSiblingsWithNoMotherAssumingTheSameMother() {
        Siblings siblings = repo.findSiblings("3");

        assertThat(siblings).isEqualTo(PERSON_3_SIBLINGS);
    }

    @Test
    void shouldFindSiblingsWithNoFatherAssumingTheSameFather() {
        Siblings siblings = repo.findSiblings("8");

        assertThat(siblings).isEqualTo(PERSON_8_SIBLINGS);
    }

    @Test
    void shouldFindAllPeopleSorted() {
        List<Person> people = repo.findPeople();
        assertThat(people).hasSize(peopleCount());
        assertThat(people.get(0).getName()).isEqualTo("First Person");
        assertThat(people.get(1).getName()).isEqualTo("Mr Test");
    }

    @Test
    void shouldUpdateEntities() {
        Person person = repo.findPerson("21").get();
        person.setYearOfBirth(1799);
        person.setYearOfDeath(1888);

        Fact fact = person.getFacts().get(0);
        fact.setImage("Updated");
        fact.setDescription("A changed description");

        repo.save(asList(person));

        Person updatedPerson = repo.findPerson("21").get();
        Fact updatedFact = person.getFacts().get(0);

        assertThat(updatedPerson.toString()).isEqualTo(person.toString());
        assertThat(updatedFact.toString()).isEqualTo(fact.toString());
    }
}