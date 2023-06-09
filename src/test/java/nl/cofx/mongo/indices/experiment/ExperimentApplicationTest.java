package nl.cofx.mongo.indices.experiment;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.IndexOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.util.StopWatch;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@SpringBootTest
class ExperimentApplicationTest {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String DATABASE_NAME = "test";
    private static final String COLLECTION_NAME = "randomData";
    private static final String INDEX_NAME = "testIndex";

    @Autowired
    private RandomDataRepository repository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    private MongoClient mongoClient;

    @BeforeEach
    void deleteIndexes() {
        deleteIndexIfExists(INDEX_NAME);
    }

    private void deleteIndexIfExists(String name) {
        var collection = mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);

        var indexes = collection.listIndexes();
        var indexNames = StreamSupport.stream(indexes.spliterator(), false)
                .map(document -> document.get("name"))
                .collect(Collectors.toSet());

        if (indexNames.contains(name)) {
            collection.dropIndex(name);
        }
    }

    @Test
    void starts() {
        // Nothing to assert or do.
    }

    @Test
    void savesEntity() {
        repository.save(randomData());
    }

    @Test
    @Disabled
    void savesEntities() {
        var batchSize = 100;
        var totalNumberOfEntities = 1_000_000;
        IntStream.range(0, totalNumberOfEntities / batchSize).forEach(batchNumber -> {
            var entities = Stream.generate(ExperimentApplicationTest::randomData)
                    .limit(batchSize)
                    .toList();
            repository.saveAll(entities);

            if (batchNumber % 500 == 0) {
                log.info("Inserting batch number {}", batchNumber);
            }
        });
    }

    @Test
    void findsAllByBoolean() {
        var stopWatch = new StopWatch();
        stopWatch.start();
        var randomData = repository.findAllByRandomBoolean(true, Pageable.ofSize(10));
        log.info("Elements in page: {}", randomData.getContent().size());
        stopWatch.stop();
        log.info("Milliseconds spent finding by boolean: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void findsAllByInt() {
        var stopWatch = new StopWatch();
        stopWatch.start();
        var randomData = repository.findAllByRandomLong(241650809, Pageable.ofSize(10));
        log.info("Elements in page: {}", randomData.getContent().size());
        stopWatch.stop();
        log.info("Milliseconds spent finding by int: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void findsAllByIntGreaterThan() {
        var stopWatch = new StopWatch();
        stopWatch.start();
        var randomData = repository.findAllByRandomLongGreaterThan(0, Pageable.ofSize(10));
        log.info("Elements in page: {}", randomData.getContent().size());
        stopWatch.stop();
        log.info("Milliseconds spent finding by int greater than: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void findsAllByString() {
        var stopWatch = new StopWatch();
        stopWatch.start();
        var randomData = repository.findAllByRandomString("d08d0f44-fe00-423b-bbf4-f0b6bb39b537", Pageable.ofSize(10));
        log.info("Elements in page: {}", randomData.getContent().size());
        stopWatch.stop();
        log.info("Milliseconds spent finding by string: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void createsIndexViaTemplate() {
        var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

        log.info("Creating index");

        var indexDefinition = new Index();
        indexDefinition.named(INDEX_NAME)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomLong", Sort.Direction.ASC);

        var stopWatch = new StopWatch();
        stopWatch.start();
        indexOps.ensureIndex(indexDefinition);
        stopWatch.stop();
        log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void canEnsureExistingIndexViaTemplate() {
        var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

        var indexDefinition = new Index();
        indexDefinition.named(INDEX_NAME)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomLong", Sort.Direction.ASC);

        log.info("Ensuring index");
        indexOps.ensureIndex(indexDefinition);
        log.info("Ensured index");
        indexOps.ensureIndex(indexDefinition);
        log.info("Ensured index again");
    }

    @Test
    void createsIndexViaTemplateInBackground() throws InterruptedException, ExecutionException {
        var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

        var completableFuture = new CompletableFuture<Void>();
        var thread = new Thread(() -> {
            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(INDEX_NAME)
                    .on("randomBoolean", Sort.Direction.ASC)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomLong", Sort.Direction.ASC);

            var stopWatch = new StopWatch();
            stopWatch.start();
            indexOps.ensureIndex(indexDefinition);
            stopWatch.stop();
            log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());

            completableFuture.complete(null);
        });

        thread.start();

        completableFuture.get();
    }

    @Test
    void createsIndexReactively() throws InterruptedException, ExecutionException {
        var indexOps = reactiveMongoTemplate.indexOps(COLLECTION_NAME);

        log.info("Creating index");

        var indexDefinition = new Index();
        indexDefinition.named(INDEX_NAME)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomLong", Sort.Direction.ASC);

        var completableFuture = new CompletableFuture<Void>();
        var stopWatch = new StopWatch();
        stopWatch.start();
        indexOps.ensureIndex(indexDefinition).subscribe(name -> {
            stopWatch.stop();
            log.info("Time to create index {}: {}", name, stopWatch.getTotalTimeMillis());

            completableFuture.complete(null);
        });

        completableFuture.get();
    }

    @Test
    void canEnsureExistingIndexReactively() throws ExecutionException, InterruptedException {
        var indexOps = reactiveMongoTemplate.indexOps(COLLECTION_NAME);

        var indexDefinition = new Index();
        indexDefinition.named(INDEX_NAME)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomLong", Sort.Direction.ASC);

        log.info("Ensuring index");
        var completableFuture = new CompletableFuture<Void>();
        indexOps.ensureIndex(indexDefinition).subscribe(firstName -> {
            log.info("Ensured index");
            indexOps.ensureIndex(indexDefinition).subscribe(secondName -> {
                log.info("Ensured index again");
                completableFuture.complete(null);
            });
        });

        completableFuture.get();
    }

    @Test
    void createsIndexViaClient() {
        var keys = new BsonDocument();
        keys.put("randomLong", new BsonInt32(1));
        keys.put("randomString", new BsonInt32(1));
        keys.put("randomBoolean", new BsonInt32(1));

        var indexOptions = new IndexOptions();
        indexOptions.name(INDEX_NAME);

        var stopWatch = new StopWatch();
        stopWatch.start();
        log.info("Creating index");
        mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).createIndex(keys, indexOptions);
        stopWatch.stop();
        log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void canCreateExistingIndexViaClient() {
        var keys = new BsonDocument();
        keys.put("randomLong", new BsonInt32(1));
        keys.put("randomString", new BsonInt32(1));
        keys.put("randomBoolean", new BsonInt32(1));

        var indexOptions = new IndexOptions();
        indexOptions.name(INDEX_NAME);

        log.info("Creating index");
        mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).createIndex(keys, indexOptions);
        log.info("Created index");
        mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).createIndex(keys, indexOptions);
        log.info("Created index again");
    }

    private static RandomData randomData() {
        return RandomData.builder()
                .randomString(randomString())
                .randomLong(randomLong())
                .randomBoolean(randomBoolean())
                .build();
    }

    private static boolean randomBoolean() {
        return SECURE_RANDOM.nextBoolean();
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private static long randomLong() {
        return SECURE_RANDOM.nextLong();
    }
}
