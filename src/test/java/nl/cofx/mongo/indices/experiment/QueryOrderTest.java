package nl.cofx.mongo.indices.experiment;

import com.mongodb.client.MongoClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.util.StopWatch;

@Slf4j
@DataMongoTest
class QueryOrderTest {

    private static final String DATABASE_NAME = "test";
    private static final String COLLECTION_NAME = "randomData";
    private static final String INDEX_NAME = "testIndex";

    @Autowired
    private RandomDataRepository repository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoClient mongoClient;

    @BeforeEach
    void deleteIndexes() {
        mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).dropIndexes();
    }

    @Nested
    class FindByStringLongAndBoolean {

        @Test
        void indexWithDifferentOrder() {
            var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(INDEX_NAME)
                    .on("randomBoolean", Sort.Direction.ASC)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomLong", Sort.Direction.ASC);

            indexOps.ensureIndex(indexDefinition);

            // 7 ms
            benchmark(this::findByStringLongAndBoolean);
        }

        @Test
        void indexWithSameOrder() {
            var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(INDEX_NAME)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomLong", Sort.Direction.ASC)
                    .on("randomBoolean", Sort.Direction.ASC);

            indexOps.ensureIndex(indexDefinition);

            // 7 ms
            benchmark(this::findByStringLongAndBoolean);
        }

        @Test
        void withoutIndex() {
            // 665 ms
            benchmark(this::findByStringLongAndBoolean);
        }

        private void findByStringLongAndBoolean() {
            var randomData = repository.findByRandomStringAndRandomLongAndRandomBoolean("de5c2b3b-5d78-4d4c-a383-2806fc93ad51", -2133562764269312294L, false);
            log.info("Found {}", randomData);
        }
    }

    @Nested
    class FindByStringAndLong {

        @Test
        void indexWithDifferentOrder() {
            var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(INDEX_NAME)
                    .on("randomBoolean", Sort.Direction.ASC)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomLong", Sort.Direction.ASC);

            indexOps.ensureIndex(indexDefinition);

            // 7 ms
            benchmark(this::findByStringAndLong);
        }

        @Test
        void indexWithSameOrder() {
            var indexOps = mongoTemplate.indexOps(COLLECTION_NAME);

            log.info("Creating index");

            var indexDefinition = new Index();
            indexDefinition.named(INDEX_NAME)
                    .on("randomString", Sort.Direction.ASC)
                    .on("randomLong", Sort.Direction.ASC)
                    .on("randomBoolean", Sort.Direction.ASC);

            indexOps.ensureIndex(indexDefinition);

            // 7 ms
            benchmark(this::findByStringAndLong);
        }

        @Test
        void withoutIndex() {
            // 665 ms
            benchmark(this::findByStringAndLong);
        }

        private void findByStringAndLong() {
            var randomData = repository.findByRandomStringAndRandomLong("de5c2b3b-5d78-4d4c-a383-2806fc93ad51", -2133562764269312294L);
            log.info("Found {}", randomData);
        }
    }

    private void benchmark(Runnable runnable) {
        var iterations = 25;
        var stopWatch = new StopWatch();
        log.info("Starting benchmark");
        stopWatch.start();
        for (var i = 0; i < iterations; ++i) {
            runnable.run();
        }
        stopWatch.stop();
        log.info("Execution time: {} ms", stopWatch.getTotalTimeMillis() / iterations);
    }
}
