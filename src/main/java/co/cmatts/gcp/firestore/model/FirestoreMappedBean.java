package co.cmatts.gcp.firestore.model;

public interface FirestoreMappedBean {
    default String tableName() {
        return Runtime.class.getClass().getSimpleName().toLowerCase();
    }

    String getId();
}
