package co.cmatts.gcp.datastore;

import co.cmatts.gcp.datastore.model.Person;
import co.cmatts.gcp.datastore.model.Siblings;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.DatastoreOptions;
import com.googlecode.objectify.ObjectifyFactory;
import com.googlecode.objectify.ObjectifyService;
import com.googlecode.objectify.Result;
import com.googlecode.objectify.util.Closeable;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.googlecode.objectify.ObjectifyService.ofy;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

public class DatastoreRepository {

    public DatastoreRepository() {
        DatastoreOptions.Builder builder = DatastoreOptions
                .newBuilder()
                .setRetrySettings(ServiceOptions.getNoRetrySettings());

        if (nonNull(System.getProperty("local.datastore.url"))) {
            builder = builder
                    .setHost(System.getProperty("local.datastore.url"))
                    .setProjectId(System.getProperty("local.project"))
                    .setCredentials(NoCredentials.getInstance());
        }

        ObjectifyService.init(new ObjectifyFactory(builder.build().getService()));
        ObjectifyService.register(Person.class);
    }

    public Optional<Person> findPerson(String id) {
        try (Closeable session = ObjectifyService.begin()) {
            Result<Person> result = ofy().load().key(ObjectifyService.key(Person.class, id));
            Person person = result.now();
            return Optional.ofNullable(person);
        }
    }

    public List<Person> findPeople() {
        try (Closeable session = ObjectifyService.begin()) {
            return ofy().load().type(Person.class).stream()
                    .sorted((a, b) -> StringUtils.compare(a.getName(), b.getName()))
                    .collect(toList());
        }
    }

    public List<Person> findPersonByFather(String id) {
        return findEntitiesByAttribute("fatherId", id);
    }

    public List<Person> findPersonByMother(String id) {
        return findEntitiesByAttribute("motherId", id);
    }

    private List<Person> findEntitiesByAttribute(String attrName, String id) {
        if (id == null) {
            return emptyList();
        }

        try (Closeable session = ObjectifyService.begin()) {
            return ofy().load().type(Person.class).filter(attrName + " =", id).list();
        }
    }

    public Siblings findSiblings(String id) {
        Optional<Person> p = findPerson(id);
        if (p.isEmpty()) {
            return new Siblings();
        }
        Person person = p.get();

        Set<Person> allSiblings = Stream
                .concat(findPersonByFather(person.getFatherId()).stream(),
                        findPersonByMother(person.getMotherId()).stream())
                .collect(Collectors.toSet());

        return new Siblings(person, allSiblings, extractParents(allSiblings));
    }

    private List<Person> extractParents(Set<Person> allSiblings) {
        List<String> parentIds = allSiblings.stream()
                .map(s -> asList(s.getFatherId(), s.getMotherId()))
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        List<Person> parents = new ArrayList<>();
        for (String parentId : parentIds) {
            parents.add(findPerson(parentId).get());
        }
        return parents.stream()
                .sorted(Comparator.comparing(Person::getId))
                .collect(toList());
    }

    public final void save(List<Person> people) {
        try (Closeable session = ObjectifyService.begin()) {
            ofy().save().entities(people).now();
        }
    }
}