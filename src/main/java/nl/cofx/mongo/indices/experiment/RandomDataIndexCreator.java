package nl.cofx.mongo.indices.experiment;

import com.mongodb.client.MongoClient;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import nl.cofx.mongo.indices.experiment.operations.MongoIndexOperations;
import nl.cofx.mongo.indices.experiment.operations.MongoIndexSpecification;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RandomDataIndexCreator {

    private static final String COLLECTION_NAME = "randomData";
    private static final String DATABASE_NAME = "test";
    private static final MongoIndexSpecification MONGO_INDEX_SPECIFICATION_1 = MongoIndexSpecification.builder()
            .definition("{ randomBoolean: 1, randomInt: 1 }")
            .build();
    private static final MongoIndexSpecification MONGO_INDEX_SPECIFICATION_2 = MongoIndexSpecification.builder()
            .definition("{ randomInt: 1, randomBoolean: 1 }")
            .unique(true)
            .build();

    private final MongoIndexOperations mongoIndexOperations;

    public RandomDataIndexCreator(MongoClient mongoClient) {
        mongoIndexOperations = new MongoIndexOperations(DATABASE_NAME, COLLECTION_NAME, mongoClient);
    }

    @PostConstruct
    public void startIndexCreation() {
        new Thread(() -> {
            createIndexes();
            findIndexes();
            deleteIndexes();
        }).start();
    }

    private void findIndexes() {
        log.info("Found index: {}", mongoIndexOperations.findIndex(MONGO_INDEX_SPECIFICATION_1));
        log.info("Found index: {}", mongoIndexOperations.findIndex(MONGO_INDEX_SPECIFICATION_2));
        log.info("Found index: {}", mongoIndexOperations.findIndex(MongoIndexSpecification.builder()
                .name("randomString")
                .build()));
        log.info("Found index: {}", mongoIndexOperations.findIndex(MongoIndexSpecification.builder()
                .unique(true)
                .build()));
    }

    private void createIndexes() {
        log.info("Creating first index");
        mongoIndexOperations.createIndex(MONGO_INDEX_SPECIFICATION_1);
        log.info("Created first index");

        log.info("Creating second index");
        mongoIndexOperations.createIndex(MONGO_INDEX_SPECIFICATION_2);
        log.info("Created second index");
    }

    private void deleteIndexes() {
        log.info("Deleting first index");
        mongoIndexOperations.deleteIndex(MONGO_INDEX_SPECIFICATION_1);
        log.info("Deleted first index");

        log.info("Deleting second index");
        mongoIndexOperations.deleteIndex(MONGO_INDEX_SPECIFICATION_2);
        log.info("Deleted second index");
    }
}
