package co.cmatts.gcp.firestore;

import co.cmatts.gcp.firestore.model.Fact;
import co.cmatts.gcp.firestore.model.FirestoreMappedBean;
import co.cmatts.gcp.firestore.model.Person;
import co.cmatts.gcp.firestore.model.Siblings;
import com.google.api.core.ApiFuture;
import com.google.cloud.NoCredentials;
import com.google.cloud.firestore.*;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

public class FirestoreRepository {
    private static final String COLLECTION_NAME_PREFIX = "firestore.example.";
    private static final int BATCH_SIZE = 25; // supports upto 500 operations

    private static Firestore client;


    private Firestore getFirestoreClient() {
        if (client != null) {
            return client;
        }

        FirestoreOptions.Builder builder = FirestoreOptions
                .getDefaultInstance()
                .toBuilder();

        if (nonNull(System.getProperty("local.firestore.url"))) {
            builder = builder
                    .setHost(System.getProperty("local.firestore.url"))
                    .setProjectId(System.getProperty("local.project"))
                    .setCredentials(NoCredentials.getInstance());
        }

        client = builder.build()
                .getService();
        return client;
    }

    private CollectionReference getFirestoreCollection(Class<? extends FirestoreMappedBean> collectionClass) {
        String collectionName;
        try {
            FirestoreMappedBean instance = collectionClass.getDeclaredConstructor().newInstance();
            collectionName = COLLECTION_NAME_PREFIX + instance.tableName();
        } catch (Exception e) {
            throw new IllegalArgumentException("getFirestoreCollection expects a FirestoreMappedBean");
        }
        return getFirestoreClient().collection(collectionName);
    }

    public Optional<Person> findPerson(String id) throws ExecutionException, InterruptedException {
        if (id == null) {
            return Optional.empty();
        }

        DocumentReference result = getFirestoreCollection(Person.class).document(id);

        ApiFuture<DocumentSnapshot> future = result.get();
        DocumentSnapshot document = future.get();
        if (document.exists()) {
            return Optional.of(document.toObject(Person.class));
        }
        return Optional.empty();
    }

    public List<Person> findPersonByFather(String id) throws ExecutionException, InterruptedException {
        return findEntitiesByAttribute(id, Person.class, "fatherId");
    }

    public List<Person> findPersonByMother(String id) throws ExecutionException, InterruptedException {
        return findEntitiesByAttribute(id, Person.class, "motherId");
    }

    public List<Fact> findFacts(String id) throws ExecutionException, InterruptedException {
        return findEntitiesByAttribute(id, Fact.class, "personId");
    }

    private <T extends FirestoreMappedBean> List<T> findEntitiesByAttribute(String id, Class<T> beanClass, String attrName) throws ExecutionException, InterruptedException {
        if (id == null) {
            return emptyList();
        }

        return getFirestoreCollection(beanClass)
                .whereEqualTo(attrName, id)
                .get()
                .get()
                .toObjects(beanClass);
    }

    public Siblings findSiblings(String id) throws ExecutionException, InterruptedException {
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

    private List<Person> extractParents(Set<Person> allSiblings) throws ExecutionException, InterruptedException {
        List<String> parentIds = allSiblings.stream()
                .map(s -> asList(s.getFatherId(), s.getMotherId()))
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        List<Person> parents = new ArrayList<>();
        for(String parentId : parentIds) {
            parents.add(findPerson(parentId).get());
        }
        return parents.stream()
                .sorted(Comparator.comparing(Person::getId))
                .collect(toList());
    }

    public List<Person> findPeople() throws ExecutionException, InterruptedException {
        return getFirestoreCollection(Person.class)
                .orderBy("name")
                .get()
                .get()
                .toObjects(Person.class);
    }

    @SafeVarargs
    public final void load(List<? extends FirestoreMappedBean>... dataLists) throws ExecutionException, InterruptedException {
        List<FirestoreMappedBean> allData = Stream.of(dataLists).flatMap(List::stream).collect(toList());
        List<List<FirestoreMappedBean>> batches = Lists.partition(allData, BATCH_SIZE);
        for (List<FirestoreMappedBean> b : batches) {
            loadBatch(b);
        }
    }

    private void loadBatch(List<FirestoreMappedBean> data) throws ExecutionException, InterruptedException {
        WriteBatch batch = getFirestoreClient().batch();

        data.forEach(entity -> batch.set(getFirestoreCollection(entity.getClass()).document(entity.getId()), entity));

        ApiFuture<List<WriteResult>> future = batch.commit();
        future.get();
    }

    public void updateEntities(List<? extends FirestoreMappedBean> entities) throws ExecutionException, InterruptedException {
        ApiFuture<Void> futureTransaction =
                getFirestoreClient().runTransaction(
                        transaction -> {
                            for (FirestoreMappedBean entity : entities) {
                                transaction.set(getFirestoreCollection(entity.getClass())
                                        .document(entity.getId()), entity);
                            }
                            return null;
                        });
        futureTransaction.get();
    }
}
