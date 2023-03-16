package nl.cofx.mongo.indices.experiment;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.IndexOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@SpringBootTest
class ExperimentApplicationTest {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    @Autowired
    private RandomDataRepository repository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Autowired
    private MongoClient mongoClient;

    @Test
    void starts() {
        // Nothing to assert or do.
    }

    @Test
    void savesEntity() {
        repository.save(randomData());
    }

    @Test
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
        var randomData = repository.findAllByRandomInt(241650809, Pageable.ofSize(10));
        log.info("Elements in page: {}", randomData.getContent().size());
        stopWatch.stop();
        log.info("Milliseconds spent finding by int: {}", stopWatch.getTotalTimeMillis());
    }

    @Test
    void findsAllByIntGreaterThan() {
        var stopWatch = new StopWatch();
        stopWatch.start();
        var randomData = repository.findAllByRandomIntGreaterThan(0, Pageable.ofSize(10));
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
    void createsIndex() {
        var indexOps = mongoTemplate.indexOps("randomData");

        var indexName = "test";
        deleteIndexIfExists(indexName);

        log.info("Creating index");

        var indexDefinition = new Index();
        indexDefinition.named(indexName)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomInt", Sort.Direction.ASC);

        var stopWatch = new StopWatch();
        stopWatch.start();
        indexOps.ensureIndex(indexDefinition);
        stopWatch.stop();
        log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());

        var indexInfo = indexOps.getIndexInfo();
        log.info("Index info: {}", indexInfo);
    }

    @Test
    void createsIndexInBackground() throws InterruptedException {
        var indexOps = mongoTemplate.indexOps("randomData");

        var indexName = "test";
        deleteIndexIfExists(indexName);

        var thread = new Thread(() -> {
            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(indexName)
                    .on("randomBoolean", Sort.Direction.ASC)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomInt", Sort.Direction.ASC);

            var stopWatch = new StopWatch();
            stopWatch.start();
            indexOps.ensureIndex(indexDefinition);
            stopWatch.stop();
            log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());
        });

        thread.start();

        for (int i = 0; i < 20; ++i) {
            var indexInfo = indexOps.getIndexInfo();
            log.info("Index info: {}", indexInfo);

            Thread.sleep(1000);
        }
    }

    private void deleteIndexIfExists(String name) {
        var indexOps = mongoTemplate.indexOps("randomData");
        if (indexOps.getIndexInfo().stream().anyMatch(indexInfo -> indexInfo.getName().equals(name))) {
            indexOps.dropIndex(name);
        }
    }

    @Test
    void createsIndexReactively() throws InterruptedException {
        var indexOps = reactiveMongoTemplate.indexOps("randomData");

        var indexName = "test";
        deleteIndexIfExists(indexName);

        log.info("Creating index");

        var indexDefinition = new Index();
        indexDefinition.named(indexName)
                .on("randomBoolean", Sort.Direction.ASC)
                .on("randomString", Sort.Direction.ASC)
                .on("randomInt", Sort.Direction.ASC);

        var stopWatch = new StopWatch();
        stopWatch.start();
        indexOps.ensureIndex(indexDefinition).subscribe(name -> {
            stopWatch.stop();
            log.info("Time to create index {}: {}", name, stopWatch.getTotalTimeMillis());
        });

        for (int i = 0; i < 20; ++i) {
            indexOps.getIndexInfo().subscribe(indexInfo -> log.info("Index info: {}", indexInfo));
            Thread.sleep(1000);
        }
    }

    @Test
    void createsIndexViaClient() throws InterruptedException {
        var indexOps = mongoTemplate.indexOps("randomData");

        var indexName = "test";
        deleteIndexIfExists(indexName);

        var keys = new BsonDocument();
        keys.put("randomInt", new BsonInt32(1));
        keys.put("randomString", new BsonInt32(1));
        keys.put("randomBoolean", new BsonInt32(1));

        var indexOptions = new IndexOptions();
        indexOptions.name(indexName);

        var stopWatch = new StopWatch();
        stopWatch.start();
        mongoClient.getDatabase("test").getCollection("randomData").createIndex(keys, indexOptions);
        log.info("Time to create index: {}", stopWatch.getTotalTimeMillis());

        indexName = "test2";
        deleteIndexIfExists(indexName);
        indexOptions.unique(true)
                .name(indexName);
        mongoClient.getDatabase("test").getCollection("randomData").createIndex(keys, indexOptions);

        for (int i = 0; i < 20; ++i) {
            var indexInfo = indexOps.getIndexInfo();
            log.info("Index info: {}", indexInfo);

            Thread.sleep(1000);
        }
    }

    private static RandomData randomData() {
        return RandomData.builder()
                .randomString(randomString())
                .randomInt(randomInt())
                .randomBoolean(randomBoolean())
                .build();
    }

    private static boolean randomBoolean() {
        return SECURE_RANDOM.nextBoolean();
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private static int randomInt() {
        return SECURE_RANDOM.nextInt();
    }
}
