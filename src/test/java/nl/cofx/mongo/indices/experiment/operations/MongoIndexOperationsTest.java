package nl.cofx.mongo.indices.experiment.operations;

import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoIndexOperationsTest {

    private static final String DATABASE_NAME = "mongo-index-test";
    private static final String COLLECTION_NAME = "collection";
    private static final String NAME = "name";
    private static final String DEFINITION = "{ first: 1, second: 1 }";
    private static final String EXPECTED_DEFINITION = "{\"first\": 1, \"second\": 1}";

    private final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:6.0.1"));

    private MongoIndexOperations mongoIndexOperations;

    @BeforeEach
    void setUp() {
        mongoDBContainer.start();

        var mongoClient = MongoClients.create(mongoDBContainer.getConnectionString());
        mongoIndexOperations = new MongoIndexOperations(DATABASE_NAME, COLLECTION_NAME, mongoClient);
    }

    @AfterEach
    void tearDown() {
        mongoDBContainer.stop();
    }

    @Test
    void createsIndex() {
        var specification = MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build();
        mongoIndexOperations.createIndex(specification);

        var foundIndex = mongoIndexOperations.findIndex(specification);
        assertThat(foundIndex).isNotNull();
        assertThat(foundIndex.getName()).isEqualTo(NAME);
        assertThat(foundIndex.getDefinition()).isEqualTo(EXPECTED_DEFINITION);
    }

    @Test
    void supportsCreatingIndexTwice() {
        var specification = MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build();
        mongoIndexOperations.createIndex(specification);
        mongoIndexOperations.createIndex(specification);

        var foundIndex = mongoIndexOperations.findIndex(specification);
        assertThat(foundIndex).isNotNull();
    }

    @Test
    void throws_givenExistingIndexWithSameName() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition("{ first: 1 }")
                .build());

        assertThatThrownBy(() -> mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition("{ second: 1 }")
                .build())).isInstanceOf(ExistingIndexHasSameName.class);
    }

    @Test
    void throws_givenExistingIndexWithDifferentName() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name("name1")
                .definition(DEFINITION)
                .build());

        assertThatThrownBy(() -> mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name("name2")
                .definition(DEFINITION)
                .build())).isInstanceOf(IndexExistsWithDifferentName.class);
    }

    @Test
    void findsIndexByName() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build());

        var foundIndex = mongoIndexOperations.findIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .build());
        assertThat(foundIndex).isNotNull();
        assertThat(foundIndex.getName()).isEqualTo(NAME);
        assertThat(foundIndex.getDefinition()).isEqualTo(EXPECTED_DEFINITION);
    }

    @Test
    void findsIndexByDefinition() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build());

        var foundIndex = mongoIndexOperations.findIndex(MongoIndexSpecification.builder()
                .definition(DEFINITION)
                .build());
        assertThat(foundIndex).isNotNull();
        assertThat(foundIndex.getName()).isEqualTo(NAME);
        assertThat(foundIndex.getDefinition()).isEqualTo(EXPECTED_DEFINITION);
    }

    @Test
    void deletesIndexByName() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build());

        var namedOnlySpecification = MongoIndexSpecification.builder()
                .name(NAME)
                .build();
        var foundIndex = mongoIndexOperations.findIndex(namedOnlySpecification);
        assertThat(foundIndex).isNotNull();

        mongoIndexOperations.deleteIndex(namedOnlySpecification);

        foundIndex = mongoIndexOperations.findIndex(namedOnlySpecification);
        assertThat(foundIndex).isNull();
    }

    @Test
    void deletesIndexByDefinition() {
        mongoIndexOperations.createIndex(MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build());

        var definitionOnlySpecification = MongoIndexSpecification.builder()
                .definition(DEFINITION)
                .build();
        var foundIndex = mongoIndexOperations.findIndex(definitionOnlySpecification);
        assertThat(foundIndex).isNotNull();

        mongoIndexOperations.deleteIndex(definitionOnlySpecification);

        foundIndex = mongoIndexOperations.findIndex(definitionOnlySpecification);
        assertThat(foundIndex).isNull();
    }

    @Test
    void createsIndexWithPreferredName_givenNoExistingIndex() {
        var specification = MongoIndexSpecification.builder()
                .name(NAME)
                .definition(DEFINITION)
                .build();
        mongoIndexOperations.createIndexWithPreferredName(specification);

        var foundIndex = mongoIndexOperations.findIndex(specification);
        assertThat(foundIndex).isNotNull();
        assertThat(foundIndex.getName()).isEqualTo(NAME);
        assertThat(foundIndex.getDefinition()).isEqualTo(EXPECTED_DEFINITION);
    }

    @Test
    void doesNotCreateIndexWithPreferredName_givenExistingIndex() {
        var existingIndex = MongoIndexSpecification.builder()
                .name("name1")
                .definition(DEFINITION)
                .build();
        mongoIndexOperations.createIndex(existingIndex);

        var indexWithPreferredName = MongoIndexSpecification.builder()
                .name("name2")
                .definition(DEFINITION)
                .build();
        mongoIndexOperations.createIndexWithPreferredName(indexWithPreferredName);

        var foundIndex = mongoIndexOperations.findIndex(existingIndex);
        assertThat(foundIndex).isNotNull();
        assertThat(foundIndex.getName()).isEqualTo("name1");
        assertThat(foundIndex.getDefinition()).isEqualTo(EXPECTED_DEFINITION);
    }
}
