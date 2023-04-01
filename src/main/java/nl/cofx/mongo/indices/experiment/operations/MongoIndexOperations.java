package nl.cofx.mongo.indices.experiment.operations;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@RequiredArgsConstructor
@Slf4j
public class MongoIndexOperations {

    private final String databaseName;
    private final String collectionName;
    private final MongoClient mongoClient;

    public void createIndex(MongoIndexSpecification specification) {
        log.info("Creating index with specification {}", specification);
        try {
            getCollection().createIndex(Objects.requireNonNull(getKeys(specification)), getIndexOptions(specification));
            log.info("Created index");
        } catch (MongoCommandException e) {
            var errorCode = e.getErrorCode();
            var message = e.getMessage();
            if (errorCode == 85) {
                log.warn("Index already exists with a different name: {}", message);
                throw new IndexExistsWithDifferentName(message);
            } else if (errorCode == 86) {
                log.warn("Index with requested name already exists: {}", message);
                throw new ExistingIndexHasSameName(message);
            } else {
                throw e;
            }
        }
    }

    private static IndexOptions getIndexOptions(MongoIndexSpecification specification) {
        return new IndexOptions()
                .name(specification.getName())
                .unique(specification.isUnique());
    }

    private static Document getKeys(MongoIndexSpecification specification) {
        var definition = specification.getDefinition();
        if (definition == null) return null;

        return Document.parse(definition);
    }

    private MongoCollection<Document> getCollection() {
        return mongoClient.getDatabase(databaseName).getCollection(collectionName);
    }

    public void deleteIndex(MongoIndexSpecification specification) {
        var collection = getCollection();

        var indexes = collection.listIndexes();
        for (var index : indexes) {
            if (indexMatchesSpecification(specification, index)) {
                collection.dropIndex(index.getString("name"));
                log.info("Deleted index {}", index);
                return;
            }
        }

        log.info("No index found matching specification {}", specification);
    }

    private boolean indexMatchesSpecification(MongoIndexSpecification specification, Document index) {
        if (specification.getName() != null && !specification.getName().equals(index.getString("name"))) return false;

        if (specification.isUnique() && !Boolean.TRUE.equals(index.getBoolean("unique"))) return false;

        var keys = getKeys(specification);
        if (keys == null) return true;

        return equalRespectingInsertionOrder(index.get("key", Document.class).entrySet(), keys.entrySet());
    }

    private boolean equalRespectingInsertionOrder(Set<Map.Entry<String, Object>> firstEntries,
            Set<Map.Entry<String, Object>> secondEntries) {
        var firstIterator = firstEntries.iterator();
        var secondIterator = secondEntries.iterator();
        while (firstIterator.hasNext() && secondIterator.hasNext()) {
            if (!Objects.equals(firstIterator.next(), secondIterator.next())) return false;
        }

        return !firstIterator.hasNext() && !secondIterator.hasNext();
    }
}
