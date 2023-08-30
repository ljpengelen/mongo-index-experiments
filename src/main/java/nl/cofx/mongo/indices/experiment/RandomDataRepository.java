package nl.cofx.mongo.indices.experiment;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface RandomDataRepository extends MongoRepository<RandomData, String> {

    Page<RandomData> findAllByRandomBoolean(boolean value, Pageable page);

    Page<RandomData> findAllByRandomLong(long value, Pageable page);

    Page<RandomData> findAllByRandomLongGreaterThan(long value, Pageable page);

    Page<RandomData> findAllByRandomString(String value, Pageable page);

    RandomData findByRandomStringAndRandomLongAndRandomBoolean(String randomString, long randomLong, boolean randomBoolean);

    RandomData findByRandomStringAndRandomLong(String randomString, long randomLong);
}
